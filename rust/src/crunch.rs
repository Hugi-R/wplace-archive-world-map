use std::collections::HashMap;
use std::fs;
use std::path::Path;
use anyhow::{self, Context};

use crate::image;
use crate::tilesdb::TileDB;

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
            if self.stop.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs_f64(TICK_RATE));
            
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

fn group_files_by_day(folder_path: &str) -> anyhow::Result<HashMap<String, Vec<String>>> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    
    for entry in fs::read_dir(folder_path)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some((date, _)) = extract_date_from_filename(filename) {
                    grouped.entry(date).or_insert_with(Vec::new).push(filename.to_string());
                }
            }
        }
    }
    
    Ok(grouped)
}

fn extract_date_from_filename(filename: &str) -> Option<(String, u32)> {
    // Pattern: diff_v37.005_2025-09-17T05.db
    // Extract YYYY-MM-DD from the filename
    if (filename.len() == 29) & filename[0..5].eq("diff_") & filename[26..].eq(".db") {
        let day = &filename[13..23];
        let hour = &filename[24..26];
        let parsed_hour = hour.parse::<u32>();
        if parsed_hour.is_err() {
            return None;
        }
        return Some((day.to_string(), parsed_hour.unwrap()));
    }
    None
}

fn apply_to_db(target_db: &mut TileDB, diff_db: &mut TileDB, x: u16, y: u16) -> anyhow::Result<()> {
    let diff_data = diff_db.get_tile(11, x as i32, y as i32).context(format!("Failed to get diff tile at x={}, y={}", x, y))?;
    let diff = image::compressed_bytes_to_paletted(&diff_data)?;

    let new = match target_db.get_tile(11, x as i32, y as i32) {
        Ok(data) => {
            let old = image::compressed_bytes_to_paletted(&data)?;
            image::apply_diff_paletted(&old, &diff)
        },
        Err(_) => {
            diff
        }
    };  
    let mut data = image::paletted_to_compressed_bytes(&new)?;
    target_db.put_tile_delayed(11, x as i32, y as i32, &mut data, 0).context(format!("Failed to put tile at x={}, y={}", x, y))?;
    Ok(())
}

pub fn apply(target_path: &Path, diff_path: &Path, workers: usize) -> anyhow::Result<()> {
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

        let mut target_db = TileDB::new(target_path, false)?;
        let mut diff_db = TileDB::new(diff_path, true)?;

        let handle = std::thread::spawn(move || {
            loop {
                let job = {
                    let rx = rx.lock().unwrap();
                    rx.recv()
                };

                match job {
                    Ok((x, y)) => {
                        apply_to_db(&mut target_db, &mut diff_db, x, y).unwrap(); // crash on failure
                        metrics.record_success();
                    },
                    Err(_) => {
                        // Channel closed, worker should exit
                        break;
                    } 
                    
                }
            }
            target_db.flush_pending_tiles().unwrap(); // Flush any remaining tiles
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

pub fn crunch_day(in_folder: &str, out_folder: &str, work_folder: &str, workers: usize) -> anyhow::Result<()>{
    let grouped_files = group_files_by_day(in_folder)?;
    for (_, files) in grouped_files {
        if files.is_empty() {
            continue;
        }
        let mut files_clone = files.clone();
        files_clone.sort_by(|a, b| {
            let (_, hour_a) = extract_date_from_filename(a).unwrap();
            let (_, hour_b) = extract_date_from_filename(b).unwrap();
            hour_a.cmp(&hour_b)
        });

        // create a copy of the first file, it will be used as base
        let last_file = files_clone.last().unwrap();
        let work_file = format!("{}/day_{}", work_folder, last_file);
        let target_file = format!("{}/day_{}", out_folder, last_file);
        // Skip if target file already exists
        if Path::new(&target_file).exists() {
            println!("Target file {} already exists, skipping.", target_file);
            continue;
        }
        let base_file = format!("{}/{}", in_folder, files_clone[0]);
        fs::copy(&base_file, &work_file)?;

        // apply all diffs to the base file
        for file in files_clone.iter().skip(1) {
            let diff_file = format!("{}/{}", in_folder, file);
            println!("Applying diff {} to {}", diff_file, work_file);
            apply(Path::new(&work_file), Path::new(&diff_file), workers)?;
        }
        // move the final file to the output folder
        fs::copy(&work_file, &target_file)?;
         // remove the work file
        fs::remove_file(&work_file)?;
        
    }
    Ok(())
}

fn extract_week(filename: &str) -> Option<f32> {
    // day_diff_v31.094_2025-08-09T22.db
    // extract 31.094
    if (filename.len() == 33) & filename[0..9].eq("day_diff_") & filename[30..].eq(".db") {
        let parts: Vec<&str> = filename[10..].split("_").collect();
        let week = parts[0].parse::<f32>();
        match week {
            Ok(week) => return Some(week),
            Err(_) => return None,
        }
    }
    None
}

pub fn crunch_week(in_folder: &str, out_folder: &str, work_folder: &str, workers: usize) -> anyhow::Result<()> {
    let mut files: HashMap<String, String> = HashMap::new();

    for entry in fs::read_dir(in_folder)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(week) = extract_week(filename) {
                    println!("Found week {} file: {}", week, filename);
                    files.insert(week.to_string(), filename.to_string());
                }
            }
        }
    }

    if files.len() < 2 {
        return Err(anyhow::anyhow!("Not enough files to crunch."));
    }

    // sort by week
    let mut weeks: Vec<f32> = files.keys().filter_map(|k| k.parse::<f32>().ok()).collect();
    weeks.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // get already done weeks
    let mut done_files: HashMap<String, String> = HashMap::new();
    for entry in fs::read_dir(out_folder)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                // filename pattern: w31_day_diff_v31.094_2025-08-09T22.db
                if filename.starts_with("w") && filename.ends_with(".db") {
                    let parts: Vec<&str> = filename[1..].split("_").collect();
                    if parts.len() >= 2 {
                        let week_str = parts[0];
                        if let Ok(week) = week_str.parse::<u32>() {
                            done_files.insert(week.to_string(), filename.to_string());
                        }
                    }

                }
            }
        }
    }
    // sort done weeks
    let mut done_weeks: Vec<f32> = done_files.keys().filter_map(|k| k.parse::<f32>().ok()).collect();
    done_weeks.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let last_week_done: f32 = if done_weeks.is_empty() {
        -1.0
    } else {
        *done_weeks.last().unwrap()
    };
    
    // skip already done weeks
    weeks = weeks.into_iter().filter(|w| w.floor() > last_week_done).collect();
    if weeks.is_empty() {
        println!("All weeks already processed.");
        return Ok(());
    }

    // create a copy of the first file, it will be used as base
    let start_file = if last_week_done == -1.0 {
        println!("No previous week found, starting from the beginning.");
        let first_week = weeks[0];
        let first_file = files.get(&first_week.to_string()).unwrap();
        let base_file = format!("{}/{}", in_folder, first_file);
        let prev_week = (first_week - 1.0).floor() as u32;
        let target_file = format!("{}/w{}_{}", out_folder, prev_week, first_file);
        fs::copy(&base_file, &target_file)?;
        target_file
    } else {
        println!("Last week done: {}, resuming from next week.", last_week_done);
        let last_done_file = done_files.get(&last_week_done.to_string()).unwrap();
        let target_file = format!("{}/{}", out_folder, last_done_file);
        target_file
    };

    let work_file = format!("{}/work_file.db", work_folder);
    fs::copy(&start_file, &work_file)?;

    let first_week = weeks[0];
    let mut current_week = first_week.floor() as u32;
    let mut last_file = start_file;
    for week in weeks.iter() {
        // save current week file if week changed
        if current_week != week.floor() as u32 {
            let week_target_file = format!("{}/w{}_{}", out_folder, current_week, last_file);
            println!("Saving week file {}", week_target_file);
            fs::copy(&work_file, &week_target_file)?;
            current_week = week.floor() as u32;
        }
        
        let file = files.get(&week.to_string()).unwrap();
        let diff_file = format!("{}/{}", in_folder, file);
        println!("Applying diff {} to {}", diff_file, work_file);
        apply(Path::new(&work_file), Path::new(&diff_file), workers)?;
        last_file = file.to_string();
    }

    Ok(())
}