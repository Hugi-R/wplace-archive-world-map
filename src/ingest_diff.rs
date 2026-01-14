use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Utc, DateTime, Timelike};
use anyhow::Context;
use wplacetools;
use wplacetools::diff::IndexEntry;

use crate::image::PalettedImage;
use crate::palette;
use crate::image;
use crate::tilesdb::TileDB;

pub struct Metrics {
    start_time: std::time::Instant,
    done: std::sync::atomic::AtomicU64,
    success: std::sync::atomic::AtomicU64,
    fail: std::sync::atomic::AtomicU64,
    last_done: std::sync::atomic::AtomicU64,
    total_job: std::sync::atomic::AtomicU64,
    stop: std::sync::atomic::AtomicBool,
}

impl Metrics {
    fn new() -> Self {
        Metrics {
            start_time: std::time::Instant::now(),
            done: std::sync::atomic::AtomicU64::new(0),
            success: std::sync::atomic::AtomicU64::new(0),
            fail: std::sync::atomic::AtomicU64::new(0),
            last_done: std::sync::atomic::AtomicU64::new(0),
            total_job: std::sync::atomic::AtomicU64::new(0),
            stop: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn add_total_job(&self, n: u64) {
        self.total_job.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_fail(&self) {
        self.done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.fail.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_success(&self) {
        self.done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.success.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn stop(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn print_metrics(&self) {
        let done = self.done.load(std::sync::atomic::Ordering::SeqCst);
        let success = self.success.load(std::sync::atomic::Ordering::SeqCst);
        let fail = self.fail.load(std::sync::atomic::Ordering::SeqCst);
        let total = self.total_job.load(std::sync::atomic::Ordering::SeqCst);
        let percent = if total > 0 {
            (done as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        
        let elapsed = self.start_time.elapsed().as_secs_f64();

        eprintln!(
            "Done: {}, Success: {}, Fail: {}. Total: {}, {:.2}%. Elapsed: {:.1}s",
            done, success, fail, total, percent, elapsed
        );
    }

    fn report_metrics(&self) {
        const TICK_RATE: f64 = 5.0;
        loop {
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs_f64(TICK_RATE));
            
            let done = self.done.load(std::sync::atomic::Ordering::SeqCst);
            let success = self.success.load(std::sync::atomic::Ordering::SeqCst);
            let fail = self.fail.load(std::sync::atomic::Ordering::SeqCst);
            let last_done = self.last_done.swap(done, std::sync::atomic::Ordering::SeqCst);
            let rate = (done - last_done) as f64 / TICK_RATE;

            let total = self.total_job.load(std::sync::atomic::Ordering::SeqCst);
            let percent = if total > 0 {
                (done as f64 / total as f64) * 100.0
            } else {
                0.0
            };
            
            let elapsed = self.start_time.elapsed().as_secs_f64();

            eprintln!(
                "Rate: {:.2}/s, Done: {}, Success: {}, Fail: {}. Total: {}, {:.2}%. Elapsed: {:.1}s",
                rate, done, success, fail, total, percent, elapsed
            );
        }
    }
}

/// convert a wplacetools diff to this project diff
fn convert_diff3(diff: &[u8]) -> Vec<u8> {
    let mut res = vec![0u8; diff.len()];
    for i in 0..diff.len() {
        let diff_index = diff[i] & wplacetools::PALETTE_INDEX_MASK;
        res[i] = palette::WPLACETOOLS_PALETTE_CONVERSION[diff_index as usize];
    }
    res
}

fn apply_to_db(db: &mut TileDB, diff: PalettedImage, x: u16, y: u16, crc32: u32) -> anyhow::Result<()> {
    let new = match db.get_tile(11, x as i32, y as i32) {
        Ok(data) => {
            let old = image::compressed_bytes_to_paletted(&data)?;
            image::apply_diff_paletted(&old, &diff)
        },
        Err(_) => {
            diff
        }
    };
    let mut data = image::paletted_to_compressed_bytes(&new)?;
    db.put_tile_delayed(11, x as i32, y as i32, &mut data, crc32)?;
    Ok(())
}

fn apply_entry<R: io::Seek + io::Read>(db: &mut TileDB, diff_file: &mut wplacetools::diff::DiffFile<R>, entry: IndexEntry) -> anyhow::Result<()> {
    let chunk = diff_file.read_chunk(&entry)?;
    let converted = convert_diff3(&chunk);
    let img = PalettedImage { width: wplacetools::CHUNK_WIDTH, height: wplacetools::CHUNK_WIDTH, indices: converted };
    apply_to_db(db, img, entry.x, entry.y, entry.checksum)?;
    Ok(())
}

pub fn apply(db_path: &Path, diff_path: &Path, workers: usize) -> anyhow::Result<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<IndexEntry>(workers*2);
    let rx = std::sync::Arc::new(std::sync::Mutex::new(rx)); // make multiple consumers from a single consumer

    let metrics = std::sync::Arc::new(Metrics::new());

    // Start metrics reporting thread
    let metrics_clone = metrics.clone();
    let _metrics_thread = std::thread::spawn(move || {
        metrics_clone.report_metrics();
    });

    // Spawn workers
    let mut worker_handles = Vec::new();
    for _ in 0..workers {
        let rx = rx.clone();
        let metrics = metrics.clone();

        let mut db = TileDB::new(db_path, false)?;
        let file = File::open(diff_path)?;
        let mut diff_file = wplacetools::diff::DiffFile::open(file)?;

        let handle = std::thread::spawn(move || {
            loop {
                let job = {
                    let rx = rx.lock().unwrap();
                    rx.recv()
                };

                match job {
                    Ok(entry) => {
                        match apply_entry(&mut db, &mut diff_file, entry) {
                            Ok(_) => metrics.record_success(),
                            Err(e) => {
                                eprintln!("Failed to apply entry x={} y={}. {}", entry.x, entry.y, e);
                                metrics.record_fail();
                            }
                        }
                    },
                    Err(_) => {
                        // Channel closed, worker should exit
                        break;
                    } 
                    
                }
            }
            db.flush_pending_tiles().unwrap(); // Flush any remaining tiles
        });
        worker_handles.push(handle);
    }

    // Send jobs
    let file = File::open(Path::new(diff_path))?;
    let mut diff_file = wplacetools::diff::DiffFile::open(file)?;
    let index = diff_file.list_index()?;
    println!("Index size: {}", index.len());
    metrics.add_total_job(index.len() as u64);
    for entry in  index {
        tx.send(entry).context("diff3 apply failed to send job")?;
    }

    // Drop the sender to signal workers to exit
    drop(tx);

    // Wait for all workers to finish
    for handle in worker_handles {
        let _ = handle.join();
    }

    metrics.stop();
    metrics.print_metrics();

    Ok(())
}

pub fn convert(out_folder: &str, diff_folder: &str, workers: usize) -> anyhow::Result<()> {

    for entry in fs::read_dir(&diff_folder)? {
        let entry = entry?;
        let diff_path = entry.path();
        if !diff_path.is_file() {
            continue;
        }

        let new_name = convert_filename(&diff_path)?;
        let out_path = Path::new(out_folder).join(new_name);
        eprintln!("Convert {} to {}", diff_path.display(), out_path.display());
        //TODO close metrics
        apply(&out_path.as_path(), diff_path.as_path(), workers)?
    }

    Ok(())
}

fn convert_filename(path: &Path) -> anyhow::Result<String> {
        let epoch = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc();

        if path.extension().and_then(|e| e.to_str()) != Some("diff") {
            return Err(anyhow::anyhow!("file {} not .diff", path.display()));
        }

        let file_name = match path.file_name().and_then(|s| s.to_str()) {
            Some(s) => s,
            None => return Err(anyhow::anyhow!("failed to get file name {}", path.display())),
        };
        // Expect: 2025-08-09T22-23-45.217Z.diff
        let stem = match file_name.strip_suffix(".diff") {
            Some(s) => s,
            None => return Err(anyhow::anyhow!("failed to remove suffix {}", path.display())),
        };

        match parse_timestamp(stem) {
            Ok(dt) => {
                let (x_weeks, y_hours) = version_xy(epoch, dt);
                let out = format!(
                    "v{}.{}_{:04}-{:02}-{:02}T{:02}.db",
                    x_weeks,
                    format!("{:03}", y_hours),
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour()
                );
                Ok(out)
            }
            Err(e) => Err(anyhow::anyhow!("failed to parse filename {}: {}", stem, e)),
        }
}

/// Parses timestamps of the form:
///   YYYY-MM-DDTHH-MM-SS.mmmZ (not RFC!)
/// into a DateTime<Utc>.
fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, String> {
    NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H-%M-%S%.fZ")
        .map_err(|e| format!("parse error for {s}: {e}")).and_then(|dt| Ok(dt.and_utc()))
}

/// X = number of full weeks since 2025-01-01 (week 0 starts at 2025-01-01T00:00)
/// Y = number of full hours since the start of week X (0..167)
fn version_xy(epoch: DateTime<Utc>, dt: DateTime<Utc>) -> (i64, i64) {
    let delta = dt - epoch;
    let total_hours = delta.num_hours();
    let x_weeks = total_hours.div_euclid(24 * 7);
    let week_start = epoch + Duration::weeks(x_weeks);
    let y_hours = (dt - week_start).num_hours();
    (x_weeks, y_hours)
}