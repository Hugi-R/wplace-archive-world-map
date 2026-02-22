use rusqlite::{Connection, params, Result as SqlResult};
use std::{path::{Path, PathBuf}, time::Duration};

use crate::{image::CompressedImage, utils::{DateHours, TileHistory}};

pub const DEFAULT_DB_PATH: &str = "./tiles.db";
pub const SQLITE_BUSY_TIMEOUT_SECS: u64 = 20;
pub const BATCH_SIZE: usize = 50;

pub struct TileDB {
    db_path: PathBuf,
    read_only: bool,
    conn: Connection,
    pending_tiles: Vec<(i32, i32, i32, Vec<u8>)>,
}

impl TileDB {
    /// Create a new TileDB instance
    pub fn new(db_path: &Path, read_only: bool) -> SqlResult<Self> {
        let conn = if read_only {
            Connection::open_with_flags(
                db_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?
        } else {
            Connection::open(db_path)?
        };

        let mut db = TileDB {
            db_path: db_path.to_path_buf(),
            read_only,
            conn,
            pending_tiles: Vec::new(),
        };

        db.init()?;
        Ok(db)
    }

    fn init(&mut self) -> SqlResult<()> {
        // Set busy timeout
        let timeout_ms = SQLITE_BUSY_TIMEOUT_SECS * 1000;
        self.conn
            .execute_batch(&format!("PRAGMA busy_timeout = {}", timeout_ms))?;

        if !self.read_only {
            self.init_write()?;
        }

        Ok(())
    }

    fn init_write(&mut self) -> SqlResult<()> {

        // Ensure schema
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tiles (
                z INTEGER NOT NULL,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (z, x, y)
            );

            CREATE TABLE IF NOT EXISTS versions (
                date INTEGER PRIMARY KEY,
                original_file TEXT
            );
            ",
        )?;

        Ok(())
    }

    /// Put a version into the database
    pub fn put_version(&mut self, date: DateHours, original_file: &str) -> SqlResult<()> {
        if self.read_only {
            return Err(rusqlite::Error::InvalidQuery);
        }
        self.conn.execute(
            "INSERT INTO versions (date, original_file) VALUES (?, ?)
             ON CONFLICT(date) DO UPDATE SET original_file=excluded.original_file",
            params![date.0, original_file],
        )?;
        Ok(())
    }

    /// Get all versions from the database, ordered by date
    pub fn get_versions(&mut self) -> SqlResult<Vec<(DateHours, String)>> {
        let mut stmt = self.conn.prepare("SELECT date, original_file FROM versions ORDER BY date")?;
        let versions = stmt.query_map([], |row| {
            Ok((DateHours(row.get::<_, u32>(0)?), row.get(1)?))
        })?;

        let mut result = Vec::new();
        for version in versions {
            result.push(version?);
        }
        Ok(result)
    }

    /// Put a tile into the database with retry logic
    pub fn put_tile(&mut self, z: i32, x: i32, y: i32, data: &[u8]) -> SqlResult<()> {
        self.put_with_retry(z, x, y, data, 5)
    }

    /// Put multiple tiles into the database in a single transaction
    pub fn put_tile_batch(&mut self, tiles: &[(i32, i32, i32, Vec<u8>)]) -> SqlResult<()> {
        let tx = self.conn.transaction()?;
        
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO tiles (z, x, y, data) VALUES (?, ?, ?, ?)
                 ON CONFLICT(z, x, y) DO UPDATE SET data=excluded.data"
            )?;
            
            for (z, x, y, data) in tiles {
                stmt.execute(params![z, x, y, data])?;
            }
        }
        
        tx.commit()?;
        Ok(())
    }

    /// Queue a tile for delayed batch insertion
    /// Automatically flushes when batch size is reached
    pub fn put_tile_delayed(&mut self, z: i32, x: i32, y: i32, data: &[u8]) -> SqlResult<()> {
        self.pending_tiles.push((z, x, y, data.to_vec()));
        
        if self.pending_tiles.len() >= BATCH_SIZE {
            // Retry up to 5 times on failure
            for attempt in 0..5 {
                match self.flush_pending_tiles() {
                    Ok(_) => break,
                    Err(e) => {
                        eprintln!(
                            "Failed to flush pending tiles (attempt {}/{}): {}",
                            attempt + 1, 5, e
                        );
                        if attempt < 4 {
                            std::thread::sleep(Duration::from_millis(5000));
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Flush any pending tiles to the database
    pub fn flush_pending_tiles(&mut self) -> SqlResult<()> {
        if self.pending_tiles.is_empty() {
            return Ok(());
        }
        
        let tiles = std::mem::take(&mut self.pending_tiles);
        self.put_tile_batch(&tiles)?;
        Ok(())
    }

    fn put_with_retry(
        &mut self,
        z: i32,
        x: i32,
        y: i32,
        data: &[u8],
        retries: usize,
    ) -> SqlResult<()> {
        for attempt in 0..retries {
            let mut stmt = self.conn.prepare_cached(
                "INSERT INTO tiles (z, x, y, data) VALUES (?, ?, ?, ?)
                 ON CONFLICT(z, x, y) DO UPDATE SET data=excluded.data"
            )?;
            
            match stmt.execute(params![z, x, y, data]) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    eprintln!(
                        "DB write error for tile ({}, {}, {}) (attempt {}/{}): {}",
                        z, x, y, attempt + 1, retries, e
                    );
                    if attempt < retries - 1 {
                        std::thread::sleep(Duration::from_millis(300));
                    }
                }
            }
        }
        Err(rusqlite::Error::InvalidQuery)
    }

    /// Get raw tile data from the database
    pub fn get_tile_raw(&mut self, z: i32, x: i32, y: i32) -> SqlResult<Vec<u8>> {
        let mut stmt = self.conn.prepare_cached("SELECT data FROM tiles WHERE z = ? AND x = ? AND y = ?")?;
        let data = stmt.query_row(params![z, x, y], |row| row.get(0))?;
        Ok(data)
    }

    pub fn update_history_tile(&mut self, z: u16, x: u16, y: u16, date: DateHours, img: &CompressedImage) -> anyhow::Result<()> {
        let mut old_history = self.get_history_tile(z, x, y)?.unwrap_or(TileHistory {
            x: x as u16,
            y: y as u16,
            imgs: std::collections::HashMap::new(),
        });
        old_history.imgs.insert(date, img.clone());
        let data = old_history.to_bytes();
        self.put_tile(z as i32, x as i32, y as i32, &data).map_err(|e| anyhow::anyhow!(e))
    }

    /// Get raw tile data from the database
    pub fn get_history_tile(&mut self, z: u16, x: u16, y: u16) -> anyhow::Result<Option<TileHistory>> {
        let mut stmt = self.conn.prepare_cached("SELECT data FROM tiles WHERE z = ? AND x = ? AND y = ?")?;
        let data: Vec<u8> = match stmt.query_row(params![z, x, y], |row| row.get(0)) {
            Ok(d) => d,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(anyhow::anyhow!(e)),
        };
        let tile_history = TileHistory::from_bytes(x, y, &data)?;
        Ok(Some(tile_history))
    }

    /// Stat a tile - returns (exists)
    pub fn stat_tile(&mut self, z: i32, x: i32, y: i32) -> SqlResult<bool> {
        let mut stmt = self.conn.prepare_cached("SELECT true FROM tiles WHERE z = ? AND x = ? AND y = ?")?;
        match stmt.query_row(params![z, x, y], |_| {
            Ok(true)
        }) {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// List tiles at zoom level z
    pub fn list_tiles(&mut self, z: i32) -> SqlResult<Vec<(u16, u16)>> {
        let mut stmt = self.conn.prepare_cached("SELECT x, y FROM tiles WHERE z = ?")?;
        let tiles = stmt.query_map(params![z], |row| {
            Ok((row.get::<_, u16>(0)?, row.get::<_, u16>(1)?))
        })?;

        let mut result = Vec::new();
        for tile in tiles {
            result.push(tile?);
        }
        Ok(result)
    }

    /// Close the database, reverting WAL if not read-only
    pub fn close(mut self) -> SqlResult<()> {
        // Flush any pending tiles before closing
        self.flush_pending_tiles()?;
        Ok(())
    }
}