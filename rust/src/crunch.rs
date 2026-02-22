use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::{self, Context};
use dashmap::DashMap;

use crate::image::{self, CompressedImage};
use crate::tilesdb::TileDB;
use crate::utils::DateHours;

pub struct Metrics {
    start_time: std::time::Instant,
    done: std::sync::atomic::AtomicU64,
    success: std::sync::atomic::AtomicU64,
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
            last_done: std::sync::atomic::AtomicU64::new(0),
            total_job: std::sync::atomic::AtomicU64::new(0),
            stop: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn add_total_job(&self, n: u64) {
        self.total_job.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
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
        let total = self.total_job.load(std::sync::atomic::Ordering::SeqCst);
        let percent = if total > 0 {
            (done as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        
        let elapsed = self.start_time.elapsed().as_secs_f64();

        eprintln!(
            "Done: {}, Success: {}. Total: {}, {:.2}%. Elapsed: {:.1}s",
            done, success, total, percent, elapsed
        );
    }

    fn report_metrics(&self) {
        const TICK_RATE: f64 = 5.0;
        loop {
            std::thread::sleep(std::time::Duration::from_secs_f64(TICK_RATE));
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            
            let done = self.done.load(std::sync::atomic::Ordering::SeqCst);
            let success = self.success.load(std::sync::atomic::Ordering::SeqCst);
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
                "Rate: {:.2}/s, Done: {}, Success: {}, Total: {}, {:.2}%. Elapsed: {:.1}s",
                rate, done, success, total, percent, elapsed
            );
        }
    }
}

fn list_diff_file(folder_path: &str, latest_date: DateHours) -> anyhow::Result<Vec<(DateHours, String)>> {
    let mut files: Vec<(DateHours, String)> = Vec::new();
    
    for entry in fs::read_dir(folder_path)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(date) = extract_date_from_filename(filename) {
                    if date > latest_date {
                        files.push((date, filename.to_string()));
                    }
                }
            }
        }
    }
    
    files.sort_by_key(|(date, _)| *date);
    Ok(files)
}

fn extract_date_from_filename(filename: &str) -> Option<DateHours> {
    // Pattern: diff_v37.005_2025-09-17T05.db
    // Extract YYYY-MM-DDTHH from the filename
    if (filename.len() == 29) & filename[0..5].eq("diff_") & filename[26..].eq(".db") {
        let chrono_happy_date = format!("{}:00", &filename[13..26]); // chrono parse needs minutes
        let date = match chrono::NaiveDateTime::parse_from_str(&chrono_happy_date, "%Y-%m-%dT%H:%M") {
            Ok(dt) => dt,
            Err(e) => {
                eprintln!("Failed to parse date from filename {}: {}", filename, e);
                return None;
            }
        };
        return Some(DateHours::from_datetime(date.and_utc()));
    }
    None
}

fn apply_to_db(target_db: &mut TileDB, diff_db: &mut TileDB, x: u16, y: u16) -> anyhow::Result<()> {
    let diff_data = diff_db.get_tile_raw(11, x as i32, y as i32).context(format!("Failed to get diff tile at x={}, y={}", x, y))?;
    let diff = image::compressed_bytes_to_paletted(&diff_data)?;

    let new = match target_db.get_tile_raw(11, x as i32, y as i32) {
        Ok(data) => {
            let old = image::compressed_bytes_to_paletted(&data)?;
            image::apply_diff_paletted(&old, &diff)
        },
        Err(_) => {
            diff
        }
    };  
    let mut data = image::paletted_to_compressed_bytes(&new)?;
    target_db.put_tile_delayed(11, x as i32, y as i32, &mut data).context(format!("Failed to put tile at x={}, y={}", x, y))?;
    Ok(())
}

fn apply_to_memory(tiles: &Arc<DashMap<(u16, u16), CompressedImage>>, diff_db: &mut TileDB, x: u16, y: u16) -> anyhow::Result<()> {
    let diff_data = diff_db.get_tile_raw(11, x as i32, y as i32).context(format!("Failed to get diff tile at x={}, y={}", x, y))?;
    let diff = image::compressed_bytes_to_paletted(&diff_data)?;

    let new = match tiles.get(&(x, y)) {
        Some(data) => {
            let old = data.to_paletted()?;
            image::apply_diff_paletted(&old, &diff)
        },
        None => {
            diff
        }
    };  
    let data = new.to_compressed_bytes()?;
    tiles.insert((x, y), data);
    Ok(())
}

pub fn apply(tiles: &Arc<DashMap<(u16, u16), CompressedImage>>, diff_path: &Path, workers: usize) -> anyhow::Result<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<(u16, u16)>(workers*2);
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
        let arc_clone = tiles.clone();

        let mut diff_db = TileDB::new(diff_path, true)?;

        let handle = std::thread::spawn(move || {
            loop {
                let job = {
                    let rx = rx.lock().unwrap();
                    rx.recv()
                };

                match job {
                    Ok((x, y)) => {
                        apply_to_memory(&arc_clone, &mut diff_db, x, y).unwrap(); // crash on failure
                        metrics.record_success();
                    },
                    Err(_) => {
                        // Channel closed, worker should exit
                        break;
                    } 
                    
                }
            }
        });
        worker_handles.push(handle);
    }

    // Send jobs
    let mut diff_db = TileDB::new(diff_path, true)?;
    let index = diff_db.list_tiles(11)?; // zoom level 11
    println!("Index size: {}", index.len());
    metrics.add_total_job(index.len() as u64);
    for entry in  index {
        tx.send(entry)?;
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

fn save_tiles_to_db(tiles: &DashMap<(u16, u16), CompressedImage>, date: DateHours, current_file: &Path) -> anyhow::Result<PathBuf> {
    let mut target_db = TileDB::new(current_file, false)?;
    target_db.put_version(date, current_file.to_string_lossy().as_ref())?;
    for item in tiles.iter() {
        let (x, y) = item.key();
        let data = item.value();
        target_db.update_history_tile(11, *x, *y, date, data)?;
    }
    let new_name = format!("w{}_{}.db", date.week(), date.0);
    let folder_name = Path::new(&current_file).parent();
    let new_path = match folder_name {
        Some(folder) => folder.join(new_name),
        None => Path::new(&new_name).to_path_buf(),
    };
    std::fs::rename(current_file, new_path.clone())?;
    Ok(new_path)
}

fn crunch_tile(base_db: &mut TileDB, target_db: &mut TileDB, x: u16, y: u16) -> anyhow::Result<()> {
    let base_history = match base_db.get_history_tile(11, x, y)? {
        Some(history) => history,
        None => return Ok(()), // no base tile, skip
    };
    let base_image = base_history.image(DateHours::max())?; // get the latest image from the history
    let base_compressed = base_image.to_compressed_bytes()?;
    // save the base image as the first entry in the history with the minimum date, so that future diffs can be applied on top of it
    target_db.update_history_tile(11, x, y, DateHours::min(), &base_compressed)?;
    Ok(())
}

fn crunch_week(base_path: &Path, target_path: &Path, workers: usize) -> anyhow::Result<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<(u16, u16)>(workers*2);
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

        let mut base_db = TileDB::new(base_path, true)?;
        let mut target_db = TileDB::new(target_path, false)?;

        let handle = std::thread::spawn(move || {
            loop {
                let job = {
                    let rx = rx.lock().unwrap();
                    rx.recv()
                };

                match job {
                    Ok((x, y)) => {
                        crunch_tile(&mut base_db, &mut target_db, x, y).unwrap(); // crash on failure
                        metrics.record_success();
                    },
                    Err(_) => {
                        // Channel closed, worker should exit
                        break;
                    } 
                    
                }
            }
        });
        worker_handles.push(handle);
    }

    // Send jobs
    let mut diff_db = TileDB::new(base_path, true)?;
    let index = diff_db.list_tiles(11)?; // zoom level 11
    println!("Index size: {}", index.len());
    metrics.add_total_job(index.len() as u64);
    for entry in  index {
        tx.send(entry)?;
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

fn make_path(folder: &str, date: DateHours) -> PathBuf {
    let filename = format!("w{}_{}.db", date.week(), date.0);
    Path::new(folder).join(filename)
}

fn extract_date_from_week_filename(filename: &str) -> Option<DateHours> {
    // Pattern: w31_5309.db
    // Extract 5309 from the filename
    if filename.starts_with("w") && filename.ends_with(".db") {
        let underscore_pos = filename.find('_').unwrap();
        let date =  filename[underscore_pos+1..filename.len()-3].to_string();
        if let Ok(date_uint) = date.parse::<u32>() {
            return Some(DateHours(date_uint));
        }
    }
    None
}

fn last_week_file(folder_path: &str) -> anyhow::Result<(DateHours, String)> {
    let mut latest_date = DateHours::min();
    let mut latest_filename = String::new();
    
    for entry in fs::read_dir(folder_path)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(date) = extract_date_from_week_filename(filename) {
                    if date > latest_date {
                        latest_date = date;
                        latest_filename = filename.to_string();
                    }
                }
            }
        }
    }
    
    Ok((latest_date, latest_filename))
}

pub fn  crunch_day(in_folder: &str, out_folder: &str, work_folder: &str, workers: usize) -> anyhow::Result<()>{
    println!("Crunching day diffs from {} to {}", in_folder, out_folder);

    let (from_date, from_file) = last_week_file(out_folder)?;
    println!("Found latest week file: {} with date {}", from_file, from_date.to_datetime());

    let tiles: Arc<DashMap<(u16, u16), CompressedImage>> = Arc::new(DashMap::new());

    // list all diff files after the latest, sorted by date
    let files = list_diff_file(in_folder, from_date)?;
    if files.len() == 0 {
        println!("No diff files found in {} after date {}", in_folder, from_date.to_datetime());
        return Ok(());
    }
    let first_file_date = files.get(0).unwrap().0;
    let current_file = make_path(work_folder, first_file_date);
    if (from_date != DateHours::min()) && (first_file_date.week() != from_date.week()) {
        let from_path = Path::new(out_folder).join(from_file);
        println!("First diff file is from week {}. Crunching base tiles into {}", first_file_date.week(), current_file.display());
        crunch_week(&from_path, &current_file, workers)?;
    }

    // apply each diff file to the in-memory hashmap, and save the result to a new week file when the week changes
    // and for each finished day we commit to the db
    let mut prev_date = first_file_date;
    let mut current_file = current_file;
    for (date, file) in files {
        // commit to db on day change
        if date.day() != prev_date.day() {
            println!("Day changed from {} to {}. Committing to db file {}", prev_date.day(), date.day(), current_file.display());
            let new_file = save_tiles_to_db(&tiles, prev_date, &current_file)?;
            tiles.clear(); // Clear the in-memory tiles after saving to db
            current_file = new_file;
            println!("Saved day file. New file: {} ", current_file.display());
            // if the week of the file is different from the latest,
            // we need to create a new week file based on the latest week file.
            if date.week() != prev_date.week() {
                println!("Week changed from {} to {}", prev_date.week(), date.week());
                println!("Saving week file {} for week {}", current_file.display(), prev_date.week());
                // copy week file to out_folder
                let current_basename = Path::new(&current_file).file_name().unwrap();
                let out_file = Path::new(out_folder).join(current_basename);
                if !Path::exists(&out_file) {
                    std::fs::copy(&current_file, &out_file)?;
                    std::fs::remove_file(&current_file)?;
                }
                current_file = make_path(work_folder, date);
                println!("Crunching new week file {} for week {}", current_file.display(), date.week());
                crunch_week(&out_file, &current_file, workers)?;
            }
        }

        println!("Processing diff file {} for date {}. Current file: {}", file, date.to_datetime(), current_file.display());
        // apply diff to the tile hashmap in memory
        let diff_file = format!("{}/{}", in_folder, file);
        println!("Applying diff {} to {}", diff_file, date.to_datetime());
        apply(&tiles, Path::new(&diff_file), workers)?;
        println!("tiles size after applying diff: {}", tiles.len());
        prev_date = date;
    }
    Ok(())
}
