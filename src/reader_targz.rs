use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use tar::{Archive, Entry, EntryType};

use crate::ingest::Job;

#[derive(Debug)]
pub enum TarGzError {
    Io(io::Error),
    UnexpectedPathStructure(String),
    ParseCoordinate(String),
    FileTooLarge { path: String, size: u64 },
    UnknownType { typeflag: u8, path: String },
}

impl From<io::Error> for TarGzError {
    fn from(err: io::Error) -> Self {
        TarGzError::Io(err)
    }
}

pub struct TarGzReader {
    archive: Archive<GzDecoder<File>>,
}

impl TarGzReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, TarGzError> {
        let file = File::open(path)?;
        let decompressor = GzDecoder::new(file);
        let archive = Archive::new(decompressor);
        
        Ok(TarGzReader { archive })
    }

    pub fn iter(&mut self) -> TarGzIterator {
        TarGzIterator {
            entries: self.archive.entries().ok(),
        }
    }

    pub fn good_jobs(&mut self) -> impl Iterator<Item = Job> + '_ {
        self.iter().filter_map(|result| result.ok())
    }
}

pub struct TarGzIterator<'a> {
    entries: Option<tar::Entries<'a, GzDecoder<File>>>,
}

fn process_entry(mut entry: Entry<GzDecoder<File>>) -> Result<Option<Job>, TarGzError> {
    let header = entry.header();
    let entry_type = header.entry_type();

    match entry_type {
        EntryType::Directory | EntryType::Symlink => {
            // Skip and continue to next entry
            Ok(None)
        }
        EntryType::Regular => {
            let path = entry.path()?.to_string_lossy().to_string();
            let path_parts: Vec<&str> = path.split('/').collect();
            let size = path_parts.len();

            if size < 2 {
                return Err(TarGzError::UnexpectedPathStructure(path));
            }

            let z = 11;
            
            let x = path_parts[size - 2]
                .parse::<i32>()
                .map_err(|_| TarGzError::ParseCoordinate(format!("x coordinate from path: {}", path)))?;

            let y_str = path_parts[size - 1]
                .strip_suffix(".png")
                .ok_or_else(|| TarGzError::ParseCoordinate(format!("y coordinate from path: {}", path)))?;
            
            let y = y_str
                .parse::<i32>()
                .map_err(|_| TarGzError::ParseCoordinate(format!("y coordinate from path: {}", path)))?;

            let file_size = header.size()?;
            if file_size > 10 * 1024 * 1024 {
                return Err(TarGzError::FileTooLarge {
                    path,
                    size: file_size,
                });
            }

            let mut data = Vec::with_capacity(file_size as usize);
            entry.read_to_end(&mut data)?;

            let crc = crc32fast::hash(&data);

            Ok(Some(Job { z, x, y, data, crc32: crc }))
        }
        _ => {
            let path = entry.path()?.to_string_lossy().to_string();
            Err(TarGzError::UnknownType {
                typeflag: entry_type.as_byte(),
                path,
            })
        }
    }
}

impl<'a> Iterator for TarGzIterator<'a> {
    type Item = Result<Job, TarGzError>;

    fn next(&mut self) -> Option<Self::Item> {
        let entries = self.entries.as_mut()?;

        loop {
            match entries.next() {
                Some(Ok(entry)) => {
                    match process_entry(entry) {
                        Ok(Some(job)) => return Some(Ok(job)),
                        Ok(None) => continue, // Skip directories/symlinks
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Err(e)) => return Some(Err(TarGzError::Io(e))),
                None => return None,
            }
        }
    }
}
