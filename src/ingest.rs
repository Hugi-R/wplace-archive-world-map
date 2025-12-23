use std::fs;
use std::io;

use crate::image;
use crate::tilesdb::TileDB;

pub struct Job {
    pub z: i32,
    pub x: i32,
    pub y: i32,
    pub data: Vec<u8>,
    pub crc32: u32,
}

pub trait Reader {
    fn read_next_good(&mut self) -> io::Result<Option<Job>>;
    fn open(&mut self, path: &str) -> io::Result<()>;
    fn close(&mut self) -> io::Result<()>;
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
    db: TileDB,
    force: bool,
    metrics: std::sync::Arc<Metrics>,
    workers: usize,
    use_diff: bool,
    base_db: Option<TileDB>,
}

impl Ingester {
    /// Create a new ingester
    pub fn new(db: TileDB, workers: usize, force: bool) -> Self {
        Ingester {
            db,
            force,
            metrics: std::sync::Arc::new(Metrics::new()),
            workers,
            use_diff: false,
            base_db: None,
        }
    }

    /// Create a diff-based ingester
    pub fn new_diff(db: TileDB, workers: usize, force: bool, base_db: TileDB) -> Self {
        let mut ingester = Ingester::new(db, workers, force);
        ingester.use_diff = true;
        ingester.base_db = Some(base_db);
        ingester
    }

    /// Process a single job
    fn process_data(&mut self, job: Job) -> io::Result<bool> {
        let (exists, _) = self.db.stat_tile(job.z, job.x, job.y)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        if (exists || false) && !self.force {
            return Ok(true); // Skip
        }

        // If diff is enabled, check CRC to quickly know if there's any change
        // if self.use_diff {
        //     if let Some(ref base_db) = self.base_db {
        //         if let Ok((base_exists, crc32)) = base_db.stat_tile(job.z, job.x, job.y) {
        //             if base_exists && crc32 == job.crc32 {
        //                 self.metrics.record_crc_skip();
        //                 return Ok(true); // Skip, no change on tile
        //             }
        //         }
        //     }
        // }

        let reader = io::Cursor::new(&job.data);
        let img_paletted = image::png_to_paletted(reader).map_err(
            |e| io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to decode tile {}/{}/{}: {}", job.z, job.x, job.y, e),
            )
        )?;

        // If diff is enabled, compute the diff
        // let final_data = if self.use_diff {
        //     if let Some(ref base_db) = self.base_db {
        //         if let Ok(base_data) = base_db.get_tile(job.z, job.x, job.y) {
        //             let (diff, has_changes) = img_mock::diff(&base_data, &img_paletted)?;
        //             if has_changes {
        //                 diff
        //             } else {
        //                 return Ok(true); // Skip, no changes on the tile
        //             }
        //         } else {
        //             img_paletted
        //         }
        //     } else {
        //         img_paletted
        //     }
        // } else {
        //     img_paletted
        // };

        let img_compressed = image::paletted_to_compressed_bytes(&img_paletted).map_err(
            |e| io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to compress tile {}/{}/{}: {}", job.z, job.x, job.y, e),
            )
        )?;

        // Store tile
        self.db.put_tile(job.z, job.x, job.y, &img_compressed, job.crc32)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        Ok(false)
    }

    /// Ingest data from a reader
    pub fn ingest(&mut self, mut reader: Box<dyn Reader>) -> io::Result<()> {
        let metrics = self.metrics.clone();
        
        // Start metrics reporting thread
        let metrics_clone = metrics.clone();
        let _metrics_thread = std::thread::spawn(move || {
            metrics_clone.report_metrics();
        });

        // Process jobs sequentially
        loop {
            match reader.read_next_good()? {
                Some(job) => {
                    self.metrics.record_read();
                    match self.process_data(job) {
                        Ok(skip) => {
                            if skip {
                                self.metrics.record_skip();
                            } else {
                                self.metrics.record_success();
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to process job: {}", e);
                            self.metrics.record_fail();
                        }
                    }
                }
                None => break,
            }
        }

        reader.close()?;
        Ok(())
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
    let db = TileDB::new(output, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("failed to create tile database: {}", e)))?;

    let mut ingester = if !base.is_empty() {
        let base_db = TileDB::new(base, true)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("failed to open base tile database: {}", e)))?;
        Ingester::new_diff(db, workers, false, base_db)
    } else {
        Ingester::new(db, workers, false)
    };

    // Choose reader based on input format
    let reader: Box<dyn Reader> = if input.ends_with(".7z") {
        return Err(io::Error::new(io::ErrorKind::Other, "7z format not yet implemented"));
    } else if is_dir(input) {
        return Err(io::Error::new(io::ErrorKind::Other, "folder format not yet implemented"));
    } else if input.ends_with(".tar.gz") || input.ends_with(".tgz") {
        let mut reader = crate::reader_targz::ReaderTarGz::new();
        reader.open(input)?;
        Box::new(reader)
    } else {
        return Err(io::Error::new(io::ErrorKind::Other, format!("unsupported input format: {}", input)));
    };

    ingester.ingest(reader)?;
    println!("Ingestion completed.");
    Ok(())
}
