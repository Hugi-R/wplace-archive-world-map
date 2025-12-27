use std::fs;
use std::io;

use crate::image;
use crate::reader_targz::TarGzReader;
use crate::tilesdb::TileDB;

pub struct Job {
    pub z: i32,
    pub x: i32,
    pub y: i32,
    pub data: Vec<u8>,
    pub crc32: u32,
}

pub struct Metrics {
    read: std::sync::atomic::AtomicU64,
    last_read: std::sync::atomic::AtomicU64,
    done: std::sync::atomic::AtomicU64,
    success: std::sync::atomic::AtomicU64,
    fail: std::sync::atomic::AtomicU64,
    skip: std::sync::atomic::AtomicU64,
    crcskip: std::sync::atomic::AtomicU64,
    last_done: std::sync::atomic::AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Metrics {
            read: std::sync::atomic::AtomicU64::new(0),
            last_read: std::sync::atomic::AtomicU64::new(0),
            done: std::sync::atomic::AtomicU64::new(0),
            success: std::sync::atomic::AtomicU64::new(0),
            fail: std::sync::atomic::AtomicU64::new(0),
            skip: std::sync::atomic::AtomicU64::new(0),
            crcskip: std::sync::atomic::AtomicU64::new(0),
            last_done: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn record_read(&self) {
        self.read.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_fail(&self) {
        self.done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.fail.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_success(&self) {
        self.done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.success.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_skip(&self) {
        self.done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.skip.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_crc_skip(&self) {
        self.crcskip.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn report_metrics(&self) {
        const TICK_RATE: f64 = 5.0;
        loop {
            std::thread::sleep(std::time::Duration::from_secs_f64(TICK_RATE));
            
            let read = self.read.load(std::sync::atomic::Ordering::SeqCst);
            let last_read = self.last_read.swap(read, std::sync::atomic::Ordering::SeqCst);
            let read_rate = (read - last_read) as f64 / TICK_RATE;
            
            let done = self.done.load(std::sync::atomic::Ordering::SeqCst);
            let success = self.success.load(std::sync::atomic::Ordering::SeqCst);
            let skip = self.skip.load(std::sync::atomic::Ordering::SeqCst);
            let fail = self.fail.load(std::sync::atomic::Ordering::SeqCst);
            let last_done = self.last_done.swap(done, std::sync::atomic::Ordering::SeqCst);
            let rate = (done - last_done) as f64 / TICK_RATE;
            let crcskip = self.crcskip.load(std::sync::atomic::Ordering::SeqCst);
            
            eprintln!(
                "Rate: {:.2}/s, Done: {}, Success: {}, Skip: {}, Fail: {}. Read rate: {:.2}, Read: {}, CrcSkip: {}",
                rate, done, success, skip, fail, read_rate, read, crcskip
            );
        }
    }
}

pub struct Ingester {
    db_path: String,
    force: bool,
    metrics: std::sync::Arc<Metrics>,
    workers: usize,
    use_diff: bool,
    base_db_path: Option<String>,
}

impl Ingester {
    /// Create a new ingester
    pub fn new(db_path: String, workers: usize, force: bool) -> Self {
        Ingester {
            db_path,
            force,
            metrics: std::sync::Arc::new(Metrics::new()),
            workers,
            use_diff: false,
            base_db_path: None,
        }
    }

    /// Create a diff-based ingester
    pub fn new_diff(db_path: String, workers: usize, force: bool, base_db_path: String) -> Self {
        let mut ingester = Ingester::new(db_path, workers, force);
        ingester.use_diff = true;
        ingester.base_db_path = Some(base_db_path);
        ingester
    }

    /// Ingest data from a reader
    pub fn ingest(&mut self, mut reader: TarGzReader) -> io::Result<()> {
        let metrics = self.metrics.clone();
        
        // Start metrics reporting thread
        let metrics_clone = metrics.clone();
        let _metrics_thread = std::thread::spawn(move || {
            metrics_clone.report_metrics();
        });

        // Create a bounded channel for distributing jobs to workers
        // Use a buffer size of ~2x workers to allow some queueing but prevent unbounded growth
        let channel_capacity = (self.workers * 2).max(4);
        let (tx, rx) = std::sync::mpsc::sync_channel::<Job>(channel_capacity);
        let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));

        // Spawn worker threads
        let mut worker_handles = vec![];
        for _ in 0..self.workers {
            let rx = rx.clone();
            let db_path = self.db_path.clone();
            let metrics = self.metrics.clone();
            let force = self.force;

            let handle = std::thread::spawn(move || {
                // Each worker opens its own database connection
                let mut db = match TileDB::new(db_path, false) {
                    Ok(db) => db,
                    Err(e) => {
                        eprintln!("Worker failed to open database: {}", e);
                        return;
                    }
                };

                loop {
                    let job = {
                        let rx = rx.lock().unwrap();
                        rx.recv()
                    };

                    match job {
                        Ok(job) => {
                            match Self::process_job(&mut db, job, force) {
                                Ok(skip) => {
                                    if skip {
                                        metrics.record_skip();
                                    } else {
                                        metrics.record_success();
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to process job: {}", e);
                                    metrics.record_fail();
                                }
                            }
                        }
                        Err(_) => {
                            // Channel closed, worker should exit
                            break;
                        }
                    }
                }
                db.flush_pending_tiles().unwrap_or(()); // Flush any remaining tiles
            });

            worker_handles.push(handle);
        }

        // Send jobs from reader to the channel
        // The bounded channel will block when full, providing backpressure
        for result in reader.iter() {
            match result {
                Ok(job) => {
                    self.metrics.record_read();
                    if tx.send(job).is_err() {
                        eprintln!("Failed to send job to worker");
                        self.metrics.record_fail();
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read job: {:?}", e);
                    self.metrics.record_fail();
                }
            }
        }

        // Drop the sender to signal workers to exit
        drop(tx);

        // Wait for all workers to finish
        for handle in worker_handles {
            let _ = handle.join();
        }

        Ok(())
    }

    /// Static method to process a single job (for use by worker threads)
    fn process_job(db: &mut TileDB, job: Job, force: bool) -> io::Result<bool> {
        let (exists, _) = db.stat_tile(job.z, job.x, job.y)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        if (exists || false) && !force {
            return Ok(true); // Skip
        }

        let reader = io::Cursor::new(&job.data);
        let img_paletted = image::png_to_paletted(reader).map_err(
            |e| io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to decode tile {}/{}/{}: {}", job.z, job.x, job.y, e),
            )
        )?;

        let img_compressed = image::paletted_to_compressed_bytes(&img_paletted).map_err(
            |e| io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to compress tile {}/{}/{}: {}", job.z, job.x, job.y, e),
            )
        )?;

        // We need to make TileDB mutable for put_tile. Since each worker has its own connection,
        // we'll use unsafe to cast away the const. This is safe because we're in a worker thread
        // with exclusive access to this TileDB instance.
        let db = db as *const TileDB as *mut TileDB;
        unsafe {
            (*db).put_tile_delayed(job.z, job.x, job.y, &img_compressed, job.crc32)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        
        Ok(false)
    }
}

pub fn is_dir(path: &str) -> bool {
    if let Ok(metadata) = fs::metadata(path) {
        metadata.is_dir()
    } else {
        false
    }
}

/// Ingest from input source into output database
pub fn ingest(input: &str, output: &str, base: &str, workers: usize) -> io::Result<()> {
    // Create the output database once to ensure it's initialized
    let _db = TileDB::new(output, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("failed to create tile database: {}", e)))?;
    drop(_db); // Close the initial connection

    let mut ingester = if !base.is_empty() {
        // Verify base database exists
        let _base_db = TileDB::new(base, true)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("failed to open base tile database: {}", e)))?;
        drop(_base_db);
        Ingester::new_diff(output.to_string(), workers, false, base.to_string())
    } else {
        Ingester::new(output.to_string(), workers, false)
    };

    // Choose reader based on input format
    let reader: TarGzReader = if input.ends_with(".tar.gz") || input.ends_with(".tgz") {
        TarGzReader::open(input).unwrap()
    } else {
        return Err(io::Error::new(io::ErrorKind::Other, format!("unsupported input format: {}", input)));
    };

    ingester.ingest(reader)?;
    println!("Ingestion completed.");
    Ok(())
}
