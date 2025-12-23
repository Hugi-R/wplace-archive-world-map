use std::io::{self, Read};
use std::fs::File;
use std::cell::RefCell;
use tar::Archive;
use flate2::read::GzDecoder;

use crate::ingest::{Job, Reader};

/// Stateful tar.gz reader that maintains decoder and archive state with iterator
pub struct ReaderTarGz {
    archive: RefCell<Option<Archive<GzDecoder<std::io::BufReader<File>>>>>,
}

impl ReaderTarGz {
    pub fn new() -> Self {
        ReaderTarGz { 
            archive: RefCell::new(None),
        }
    }
}

impl Reader for ReaderTarGz {
    fn open(&mut self, path: &str) -> io::Result<()> {
        let file = std::io::BufReader::new(File::open(path)?);
        let decoder = GzDecoder::new(file);
        self.archive = RefCell::new(Some(Archive::new(decoder)));
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        self.archive = RefCell::new(None);
        Ok(())
    }

    fn read_next_good(&mut self) -> io::Result<Option<Job>> {
        println!("read_next_good");
        loop {
            match self.read_one() {
                Ok(Some((job, should_continue))) => {
                    println!("Read job: z={}, x={}, y={}, data_len={}, crc32={:08x}", job.z, job.x, job.y, job.data.len(), job.crc32);
                    if !should_continue {
                        // Continue looping to find next valid entry
                        continue;
                    }
                    return Ok(Some(job));
                }
                Ok(None) => {
                    // No more entries in archive
                    println!("No more entries in archive");
                    return Ok(None);
                }
                Err(e) => {
                    // Log error and continue to next entry
                    eprintln!("Error reading entry: {}", e);
                    continue;
                }
            }
        }
    }
}

impl ReaderTarGz {
    fn read_one(&mut self) -> io::Result<Option<(Job, bool)>> {
        println!("read_one");
        let mut archive_ref = self.archive.borrow_mut();
        let archive = archive_ref.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "archive not opened")
        })?;

        // Iterate through entries, maintaining state
        if let Ok(mut entries) = archive.entries() {
            while let Some(entry_result) = entries.next() {
                let mut entry = entry_result?;
                let header = entry.header();
                println!("Processing entry: {:?}", entry.path()?);

                match header.entry_type() {
                    tar::EntryType::Directory | tar::EntryType::Symlink => {
                        // Skip directories and symlinks
                        continue;
                    }
                    tar::EntryType::Regular => {
                        let path = entry.path()?.to_string_lossy().to_string();
                        let path_parts: Vec<&str> = path.split('/').collect();
                        let size = path_parts.len();

                        if size < 2 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("unexpected file path structure: {}", path),
                            ));
                        }

                        let z = 11;
                        let x: i32 = path_parts[size - 2].parse().map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("failed to parse x coordinate from path"),
                            )
                        })?;

                        let y_str = path_parts[size - 1];
                        let y_str = if y_str.ends_with(".png") {
                            &y_str[..y_str.len() - 4]
                        } else {
                            y_str
                        };
                        let y: i32 = y_str.parse().map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("failed to parse y coordinate from path"),
                            )
                        })?;

                        let size_bytes = header.size().map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "failed to get file size")
                        })?;

                        if size_bytes > 10 * 1024 * 1024 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("file {} size too large: {} bytes", path, size_bytes),
                            ));
                        }

                        let mut data = Vec::new();
                        entry.read_to_end(&mut data)?;

                        let crc = crc32fast::hash(&data);

                        return Ok(Some((
                            Job {
                                z,
                                x,
                                y,
                                data,
                                crc32: crc,
                            },
                            true,
                        )));
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unknown entry type",
                        ));
                    }
                }
            }
        }

        Ok(None)
    }
}
