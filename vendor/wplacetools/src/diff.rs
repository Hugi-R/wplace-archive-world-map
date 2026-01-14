// Taken From https://github.com/bczhc/wplace-tools
// 
// MIT License
// 
// Copyright (c) 2025 Zhai Can
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// src/diff3.rs - 30d8f1b - 2026-01-03
//! Diff format 3.
//!
//! Optimized for binary search with fixed-length index entries (24 bytes).
//!
//! ## Format
//! Magic (11B) | Version (u16) | IndexPos (u64) | EntryCount (u32) | Metadata | Diff Data | Sorted Index Entries...

// Hugi's notes:
// - a Chunk is the original 1000x1000 image from wplace (what I call a tile). Same coordinate system as wplace.
// - Diff Data is a 1000x1000 u8 slice (the diff image, aka Chunk), where any pixel != 0 is the new pixel palette index with MUTATION_MASK.
// - Sorted Index Entries list all modified chunks.
// - No idea what's inside Metadata, I'm not sure it matters for my use.

use crate::ChunkNumber;
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
// use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Take, Write};
use std::path::Path;

pub const MAGIC: [u8; 11] = *b"wplace-diff";
pub const VERSION: u16 = 4;
pub const INDEX_ENTRY_SIZE: u64 = 24;

// ADDED
#[inline(always)]
pub fn zstd_decompress(reader: impl Read, buf: &mut [u8]) -> io::Result<()> {
    zstd::Decoder::new(reader)?.read_exact(buf)
}

// #[derive(Default, Serialize, Deserialize)]
pub struct Metadata {}

/// Fixed-size index entry (24 bytes)
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    pub x: u16,
    pub y: u16,
    pub checksum: u32,
    pub pos: u64,
    /// Length of the compression diff data
    pub len: u64,
}

impl IndexEntry {
    /// Determine if chunk is changed by checking data range
    pub const fn is_changed(&self) -> bool {
        self.pos != 0 || self.len != 0
    }
}

pub struct DiffFile<R: Read + Seek> {
    reader: R,
    /// Position of entries area
    pub index_pos: u64,
    pub entry_count: u32,
    pub metadata: Metadata,
}

impl DiffFile<BufReader<File>> {
    pub fn open_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        Self::open(BufReader::new(file))
    }
}

impl<R: Read + Seek> DiffFile<R> {
    // ADDED
    /// open and decompress a chunk into the diff image
    pub fn read_chunk(&mut self, entry: &IndexEntry) -> io::Result<Vec<u8>> {
        let mut diff_data = vec![0u8; crate::CHUNK_LENGTH];
        let reader = self.open_chunk(entry)?;
        zstd_decompress(reader, &mut diff_data)?;
        Ok(diff_data)
    }

    pub fn open(mut reader: R) -> anyhow::Result<Self> {
        // 1. Verify Magic
        let mut magic = [0_u8; MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(anyhow::anyhow!("Magic error"));
        }

        // 2. Verify Version
        let version = reader.read_u16::<LE>()?;
        if version != VERSION {
            return Err(anyhow::anyhow!("Unsupported version: {}", version));
        }

        // 3. Read Pointers
        let index_pos = reader.read_u64::<LE>()?;
        let entry_count = reader.read_u32::<LE>()?;

        // 4. Read Metadata (u32 length + JSON)
        let meta_len = reader.read_u32::<LE>()? as usize;
        let mut meta_buf = vec![0_u8; meta_len];
        reader.read_exact(&mut meta_buf)?;
        // let metadata = serde_json::from_slice(&meta_buf)?;
        let metadata = Metadata {  };

        Ok(Self {
            reader,
            index_pos,
            entry_count,
            metadata,
        })
    }

    pub fn open_chunk(&mut self, entry: &IndexEntry) -> io::Result<Take<&mut R>> {
        self.reader.seek(SeekFrom::Start(entry.pos))?;
        Ok(self.reader.by_ref().take(entry.len))
    }

    /// Perform binary search on the fixed-length index area
    pub fn query_chunk(&mut self, target: ChunkNumber) -> io::Result<Option<IndexEntry>> {
        let mut low = 0_u64;
        let mut high = self.entry_count as u64 - 1;

        while low <= high {
            let mid = (low + high) / 2;
            self.reader
                .seek(SeekFrom::Start(self.index_pos + mid * INDEX_ENTRY_SIZE))?;

            let entry = self.read_entry_at_current()?;
            let current_coord = (entry.x, entry.y);

            if current_coord == target {
                return Ok(Some(entry));
            } else if current_coord < target {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        Ok(None)
    }

    fn read_entry_at_current(&mut self) -> io::Result<IndexEntry> {
        let x = self.reader.read_u16::<LE>()?;
        let y = self.reader.read_u16::<LE>()?;
        let checksum = self.reader.read_u32::<LE>()?;
        let pos = self.reader.read_u64::<LE>()?;
        let len = self.reader.read_u64::<LE>()?;

        Ok(IndexEntry {
            x,
            y,
            checksum,
            pos,
            len,
        })
    }

    /// Collects all index entries from the diff3 file into a HashMap. Skip empty chunks.
    /// Key: ChunkNumber (x, y), Value: IndexEntry
    pub fn collect_index(&mut self) -> anyhow::Result<HashMap<ChunkNumber, IndexEntry>> {
        let mut map = HashMap::with_capacity(self.entry_count as usize);

        self.reader.seek(SeekFrom::Start(self.index_pos))?;

        for _ in 0..self.entry_count {
            let x = self.reader.read_u16::<LE>()?;
            let y = self.reader.read_u16::<LE>()?;
            let checksum = self.reader.read_u32::<LE>()?;
            let pos = self.reader.read_u64::<LE>()?;
            let len = self.reader.read_u64::<LE>()?;

            if len == 0 {
                continue;
            }

            let n: ChunkNumber = (x, y);
            let entry = IndexEntry {
                x,
                y,
                checksum,
                pos,
                len,
            };

            map.insert(n, entry);
        }

        Ok(map)
    }

    /// Collects all index entries from the diff3 file into a HashMap. Skip empty chunks.
    /// Key: ChunkNumber (x, y), Value: IndexEntry
    pub fn list_index(&mut self) -> anyhow::Result<Vec<IndexEntry>> {
        let mut index: Vec<IndexEntry> = Vec::new();

        self.reader.seek(SeekFrom::Start(self.index_pos))?;

        for _ in 0..self.entry_count {
            let x = self.reader.read_u16::<LE>()?;
            let y = self.reader.read_u16::<LE>()?;
            let checksum = self.reader.read_u32::<LE>()?;
            let pos = self.reader.read_u64::<LE>()?;
            let len = self.reader.read_u64::<LE>()?;

            if len == 0 {
                continue;
            }

            let entry = IndexEntry {
                x,
                y,
                checksum,
                pos,
                len,
            };

            index.push(entry);
        }

        index.sort_by_key(|x| x.pos);
        Ok(index)
    }

}

pub struct DiffFileWriter<W: Write + Seek> {
    writer: W,
    current_diff_data_pos: u64,
    index_entries: Vec<IndexEntry>,
}

impl<W: Write + Seek> DiffFileWriter<W> {
    pub fn create(mut writer: W, metadata: Metadata) -> anyhow::Result<Self> {
        writer.write_all(&MAGIC)?;
        writer.write_u16::<LE>(VERSION)?;
        writer.write_u64::<LE>(0)?; // IndexPos placeholder
        writer.write_u32::<LE>(0)?; // EntryCount placeholder

        // Write Metadata
        // let json = serde_json::to_vec(&metadata)?;
        let json = vec![0u8; 0];
        writer.write_u32::<LE>(json.len() as u32)?;
        writer.write_all(&json)?;

        let diff_data_pos = writer.stream_position()?;

        Ok(Self {
            writer,
            current_diff_data_pos: diff_data_pos,
            index_entries: Vec::new(),
        })
    }

    /// Add a chunk entry to the diff archive.
    ///
    /// None compressed_diff_data indicates an unchanged chunk.
    pub fn add_entry(
        &mut self,
        n: ChunkNumber,
        compressed_diff_data: Option<&[u8]>,
        chunk_checksum: u32,
    ) -> anyhow::Result<()> {
        let (pos, len) = match compressed_diff_data {
            Some(data) => {
                let start_pos = self.current_diff_data_pos;
                let data_len = data.len() as u64;
                self.writer.write_all(data)?;
                self.current_diff_data_pos += data_len;
                (start_pos, data_len)
            }
            None => (0, 0), // Unchanged status
        };

        self.index_entries.push(IndexEntry {
            x: n.0,
            y: n.1,
            checksum: chunk_checksum,
            pos,
            len,
        });

        Ok(())
    }

    pub fn finalize(mut self) -> anyhow::Result<()> {
        // 1. Sort entries by (x, y) to enable binary search
        self.index_entries.sort_by_key(|e| (e.x, e.y));

        let index_offset = self.writer.stream_position()?;
        let entry_count = self.index_entries.len() as u32;

        // 2. Write Index Entries
        for e in &self.index_entries {
            self.writer.write_u16::<LE>(e.x)?;
            self.writer.write_u16::<LE>(e.y)?;
            self.writer.write_u32::<LE>(e.checksum)?;
            self.writer.write_u64::<LE>(e.pos)?;
            self.writer.write_u64::<LE>(e.len)?;
        }

        // 3. Update Header placeholders
        // IndexPos is at offset MAGIC.len() + VERSION.len()
        let header_pos = MAGIC.len() as u64 + 2;
        self.writer.seek(SeekFrom::Start(header_pos))?;
        self.writer.write_u64::<LE>(index_offset)?;
        self.writer.write_u32::<LE>(entry_count)?;

        Ok(())
    }
}