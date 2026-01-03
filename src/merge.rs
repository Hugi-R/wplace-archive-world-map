use std::error::Error;

use rayon::prelude::*;

use crate::image::{PalettedImage, apply_diff_paletted, compressed_bytes_to_paletted, diff_paletted, downscale_mode_weighted_2x2, merge_2x2, paletted_to_compressed_bytes};
use crate::tilesdb::TileDB;

const WEIGHTS: [u32; 256] = {
    let mut weights = [100u32; 256];
    weights[0] = 0; // don't care about transparent pixels
    weights[1] = 50; // reduce importance of black pixels
    weights
};

pub struct Metrics {
    start_time: std::time::Instant,
    done: std::sync::atomic::AtomicU64,
    success: std::sync::atomic::AtomicU64,
    fail: std::sync::atomic::AtomicU64,
    last_done: std::sync::atomic::AtomicU64,
    total_job: std::sync::atomic::AtomicU64,
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

pub fn build_job_mask(db: &mut TileDB, maxz: i32) -> Result<Vec<Vec<Vec<bool>>>, Box<dyn Error>> {
    // For maxz=11 job_mask should be ~8MB (bools are 1 byte each)
    // We could do some bit packing, but current max size is no problem
    let mut job_mask = Vec::new();
    for z in 0..=maxz {
        let len = 2<<z;
        job_mask.push(vec![vec![false; len]; len]);
    }
    // Initialize job mask with all tiles at maxz
    let tiles = db.list_tiles(maxz).unwrap();
    for (x, y) in tiles {
        job_mask[maxz as usize][y as usize][x as usize] = true;
    }
    // Propagate the job mask upwards
    for z in (1..=maxz).rev() {
        let len = 2<<z;
        for y in 0..len {
            for x in 0..len {
                if job_mask[z as usize][y as usize][x as usize] {
                    job_mask[(z-1) as usize][(y/2) as usize][(x/2) as usize] = true;
                }
            }
        }
    }
    Ok(job_mask)
}

pub fn get_resized_tile(db: &mut TileDB, z: i32, x: i32, y: i32) -> Result<PalettedImage, Box<dyn Error>> {
    let data = db.get_tile(z, x, y)?;
    let paletted = compressed_bytes_to_paletted(&data)?;
    let resized = downscale_mode_weighted_2x2(&paletted, &WEIGHTS);
    Ok(resized)
}

pub fn get_resized_tile_diff(db: &mut TileDB, base_db: &mut TileDB, z: i32, x: i32, y: i32) -> Result<PalettedImage, Box<dyn Error>> {
    let data = db.get_tile(z, x, y)?;
    let paletted = compressed_bytes_to_paletted(&data)?;
    match base_db.get_tile(z, x, y) {
        Err(_) => {
            let resized = downscale_mode_weighted_2x2(&paletted, &WEIGHTS);
            Ok(resized)
        },
        Ok(base_data) => {
            let base_paletted = compressed_bytes_to_paletted(&base_data)?;
            let undiffed = apply_diff_paletted(&base_paletted, &paletted);
            let resized = downscale_mode_weighted_2x2(&undiffed, &WEIGHTS);
            Ok(resized)
        }
    }
}

fn merge_job_recursive(metrics: std::sync::Arc<Metrics>, db: &mut TileDB, z: i32, x: i32, y: i32, job_mask: &Vec<Vec<Vec<bool>>>) -> Result<PalettedImage, Box<dyn Error>> {
    if job_mask[z as usize][y as usize][x as usize] == false {
        return Ok(PalettedImage { width: 500, height: 500, indices: vec![0u8; 500*500] });
    }
    if z==11 {
        return get_resized_tile(db, z, x, y);
    }

    let p1 = merge_job_recursive(metrics.clone(), db, z+1, x*2, y*2, job_mask)?;
    let p2 = merge_job_recursive(metrics.clone(), db, z+1, x*2+1, y*2, job_mask)?;
    let p3 = merge_job_recursive(metrics.clone(), db, z+1, x*2, y*2+1, job_mask)?;
    let p4 = merge_job_recursive(metrics.clone(), db, z+1, x*2+1, y*2+1, job_mask)?;

    let merged = merge_2x2(&p1, &p2, &p3, &p4);
    if z != 10 {
        // Skip storing layer 10 tiles to save space
        // Layer 10 can be quickly generated from layer 11 clientside on-the-fly
        let compressed = paletted_to_compressed_bytes(&merged)?;
        db.put_tile_delayed(z, x as i32, y as i32, &compressed, 0)?;
    }
    metrics.record_success();

    let resized = downscale_mode_weighted_2x2(&merged, &WEIGHTS);
    Ok(resized)
}

fn merge_job_recursive_diff(metrics: std::sync::Arc<Metrics>, db: &mut TileDB, base_db: &mut TileDB, maxz: i32, z: i32, x: i32, y: i32, job_mask: &Vec<Vec<Vec<bool>>>) -> Result<PalettedImage, Box<dyn Error>> {
    if job_mask[z as usize][y as usize][x as usize] == false {
        return match get_resized_tile(base_db, z, x, y) {
            Err(_) => Ok(PalettedImage { width: 500, height: 500, indices: vec![0u8; 500*500] }),
            Ok(img) => Ok(img)
        };
    }
    if z==maxz {
        return get_resized_tile_diff(db, base_db, z, x, y);
    }

    let p1 = merge_job_recursive_diff(metrics.clone(), db, base_db, maxz, z+1, x*2, y*2, job_mask)?;
    let p2 = merge_job_recursive_diff(metrics.clone(), db, base_db, maxz, z+1, x*2+1, y*2, job_mask)?;
    let p3 = merge_job_recursive_diff(metrics.clone(), db, base_db, maxz, z+1, x*2, y*2+1, job_mask)?;
    let p4 = merge_job_recursive_diff(metrics.clone(), db, base_db, maxz, z+1, x*2+1, y*2+1, job_mask)?;

    let merged = merge_2x2(&p1, &p2, &p3, &p4);
    // if z != 10 {
    //     // Skip storing layer 10 tiles to save space
    //     // Layer 10 can be quickly generated from layer 11 clientside on-the-fly

        match base_db.get_tile(z, x as i32, y as i32) {
            Err(_) => {
                let compressed = paletted_to_compressed_bytes(&merged)?;
                db.put_tile_delayed(z, x as i32, y as i32, &compressed, 0)?;
            },
            Ok(data) => {
                let base_paletted = compressed_bytes_to_paletted(&data)?;
                let (has_changed, diff) = diff_paletted(&base_paletted, &merged);
                if has_changed {
                    let compressed = paletted_to_compressed_bytes(&diff)?;
                    db.put_tile_delayed(z, x as i32, y as i32, &compressed, 0)?;
                }
            }
        };
    // }
    metrics.record_success();

    let resized = downscale_mode_weighted_2x2(&merged, &WEIGHTS);
    Ok(resized)
}

fn finish_jobs_recursive( metrics: std::sync::Arc<Metrics>, db: &mut TileDB, tiles: &Vec<PalettedImage>, maxz: i32, z: i32, x: i32, y: i32) -> Result<PalettedImage, Box<dyn Error>> {
    if z == maxz {
        return Ok(tiles[(x + y * (2<<z)) as usize].clone());
    }

    let p1 = finish_jobs_recursive(metrics.clone(), db, tiles, maxz, z+1, x*2, y*2)?;
    let p2 = finish_jobs_recursive(metrics.clone(), db, tiles, maxz, z+1, x*2+1, y*2)?;
    let p3 = finish_jobs_recursive(metrics.clone(), db, tiles, maxz, z+1, x*2, y*2+1)?;
    let p4 = finish_jobs_recursive(metrics.clone(), db, tiles, maxz, z+1, x*2+1, y*2+1)?;

    let merged = merge_2x2(&p1, &p2, &p3, &p4);
    let compressed = paletted_to_compressed_bytes(&merged)?;
    db.put_tile_delayed(z, x as i32, y as i32, &compressed, 0)?;
    metrics.record_success();

    let resized = downscale_mode_weighted_2x2(&merged, &WEIGHTS);
    Ok(resized)
}

pub fn merge(input: &str, workers: usize) -> Result<(), Box<dyn Error>> {
    if workers > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(workers).build_global().unwrap();
    }

    let mut db = TileDB::new(input, false)?;
    let job_mask = build_job_mask(&mut db, 11)?;
    let total_jobs = job_mask.iter().map(|layer| {
        layer.iter().map(|row| {
            row.iter().filter(|&&b| b).count() as u64
        }).sum::<u64>()
    }).sum::<u64>();
    let z11_jobs: u64 = job_mask[11].iter().map(|row| {
        row.iter().filter(|&&b| b).count() as u64
    }).sum();
    let merge_jobs = total_jobs - z11_jobs;
    eprintln!("Total merge jobs to process: {}", merge_jobs);

    let metrics = std::sync::Arc::new(Metrics::new());
    metrics.add_total_job(merge_jobs);

    // Start metrics reporting thread
    let metrics_clone = metrics.clone();
    let _metrics_thread = std::thread::spawn(move || {
        metrics_clone.report_metrics();
    });

    let z_level_parallelism = 4; // 256 parallel jobs, since job duration can vary, more jobs helps keep all workers busy
    let initial_jobs = {
        let mut jobs = Vec::new();
        let z = z_level_parallelism;
        let len = 2<<z;
        for y in 0..len {
            for x in 0..len {
                jobs.push((z, x as i32, y as i32));
            }
        }
        jobs
    };

    let tiles: Vec<PalettedImage> = initial_jobs.par_iter().map(|&(z, x, y)| {
        if job_mask[z as usize][y as usize][x as usize] == false {
            return PalettedImage { width: 500, height: 500, indices: vec![0u8; 500*500] };
        }
        // Each thread gets its own TileDB instance to avoid locking issues. SQLite can handle concurrency fine in the same process.
        let mut local_db = TileDB::new(input, false).unwrap();
        let res = match merge_job_recursive(metrics.clone(), &mut local_db, z, x, y, &job_mask) {
            Ok(img) => img,
            Err(e) => {
                eprintln!("Error processing tile z={}, x={}, y={}: {}", z, x, y, e);
                metrics.record_fail();
                PalettedImage { width: 500, height: 500, indices: vec![0u8; 500*500] }
            }
        };
        local_db.flush_pending_tiles().unwrap_or_else(|e| {
            eprintln!("Error flushing local TileDB tiles: {}", e);
        });
        res
    }).collect();
    finish_jobs_recursive(metrics.clone(), &mut db, &tiles, z_level_parallelism, 0, 0, 0)?;
    
    metrics.print_metrics();
    db.close()?;
    Ok(())
}

pub fn merge_diff(input: &str, base: &str, workers: usize) -> Result<(), Box<dyn Error>> {
    if workers > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(workers).build_global().unwrap();
    }

    let mut db = TileDB::new(input, false)?;
    let job_mask = build_job_mask(&mut db, 11)?;
    let total_jobs = job_mask.iter().map(|layer| {
        layer.iter().map(|row| {
            row.iter().filter(|&&b| b).count() as u64
        }).sum::<u64>()
    }).sum::<u64>();
    let z11_jobs: u64 = job_mask[11].iter().map(|row| {
        row.iter().filter(|&&b| b).count() as u64
    }).sum();
    let merge_jobs = total_jobs - z11_jobs;
    eprintln!("Total merge jobs to process: {}", merge_jobs);

    let metrics = std::sync::Arc::new(Metrics::new());
    metrics.add_total_job(merge_jobs);

    // Start metrics reporting thread
    let metrics_clone = metrics.clone();
    let _metrics_thread = std::thread::spawn(move || {
        metrics_clone.report_metrics();
    });

    let z_level_parallelism = 4; // 256 parallel jobs, since job duration can vary, more jobs helps keep all workers busy
    let initial_jobs = {
        let mut jobs = Vec::new();
        let z = z_level_parallelism;
        let len = 2<<z;
        for y in 0..len {
            for x in 0..len {
                jobs.push((z, x as i32, y as i32));
            }
        }
        jobs
    };

    let _: Vec<PalettedImage> = initial_jobs.par_iter().map(|&(z, x, y)| {
        if job_mask[z as usize][y as usize][x as usize] == false {
            return PalettedImage { width: 500, height: 500, indices: vec![0u8; 500*500] };
        }
        // Each thread gets its own TileDB instance to avoid locking issues. SQLite can handle concurrency fine in the same process.
        let mut local_db = TileDB::new(input, false).unwrap();
        let mut base_db = TileDB::new(base, true).unwrap();
        let res = match merge_job_recursive_diff(metrics.clone(), &mut local_db, &mut base_db, 11, z, x, y, &job_mask) {
            Ok(img) => img,
            Err(e) => {
                eprintln!("Error processing tile z={}, x={}, y={}: {}", z, x, y, e);
                metrics.record_fail();
                PalettedImage { width: 500, height: 500, indices: vec![0u8; 500*500] }
            }
        };
        local_db.flush_pending_tiles().unwrap_or_else(|e| {
            eprintln!("Error flushing local TileDB tiles: {}", e);
        });
        res
    }).collect();

    let mut base_db = TileDB::new(base, true)?;
    merge_job_recursive_diff(metrics.clone(), &mut db, &mut base_db, z_level_parallelism, 0, 0, 0, &job_mask)?;
    
    metrics.print_metrics();
    db.close()?;
    Ok(())
}