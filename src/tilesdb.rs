use rusqlite::{Connection, params, Result as SqlResult};
use std::{path::{Path, PathBuf}, time::Duration};

pub const DEFAULT_DB_PATH: &str = "./tiles.db";
pub const SQLITE_BUSY_TIMEOUT_SECS: u64 = 20;
pub const BATCH_SIZE: usize = 50;

pub struct TileDB {
    db_path: PathBuf,
    read_only: bool,
    conn: Connection,
    pending_tiles: Vec<(i32, i32, i32, Vec<u8>, u32)>,
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
        // Enable WAL mode for better concurrency
        self.conn.execute_batch("PRAGMA journal_mode = WAL")?;

        // Set WAL journal size limit to 500MB
        self.conn
            .execute_batch("PRAGMA journal_size_limit = 524288000")?;

        // Ensure schema
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tiles (
                z INTEGER NOT NULL,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                crc32 INTEGER,
                data BLOB NOT NULL,
                PRIMARY KEY (z, x, y)
            )",
        )?;

        Ok(())
    }

    /// Put a tile into the database with retry logic
    pub fn put_tile(&mut self, z: i32, x: i32, y: i32, data: &[u8], crc32: u32) -> SqlResult<()> {
        self.put_with_retry(z, x, y, data, crc32, 5)
    }

    /// Put multiple tiles into the database in a single transaction
    pub fn put_tile_batch(&mut self, tiles: &[(i32, i32, i32, Vec<u8>, u32)]) -> SqlResult<()> {
        let tx = self.conn.transaction()?;
        
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO tiles (z, x, y, crc32, data) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(z, x, y) DO UPDATE SET data=excluded.data,crc32=excluded.crc32"
            )?;
            
            for (z, x, y, data, crc32) in tiles {
                stmt.execute(params![z, x, y, *crc32 as i64, data])?;
            }
        }
        
        tx.commit()?;
        Ok(())
    }

    /// Queue a tile for delayed batch insertion
    /// Automatically flushes when batch size is reached
    pub fn put_tile_delayed(&mut self, z: i32, x: i32, y: i32, data: &[u8], crc32: u32) -> SqlResult<()> {
        self.pending_tiles.push((z, x, y, data.to_vec(), crc32));
        
        if self.pending_tiles.len() >= BATCH_SIZE {
            self.flush_pending_tiles()?;
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
        crc32: u32,
        retries: usize,
    ) -> SqlResult<()> {
        for attempt in 0..retries {
            let mut stmt = self.conn.prepare_cached(
                "INSERT INTO tiles (z, x, y, crc32, data) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(z, x, y) DO UPDATE SET data=excluded.data,crc32=excluded.crc32"
            )?;
            
            match stmt.execute(params![z, x, y, crc32 as i64, data]) {
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

    /// Get tile data from the database
    pub fn get_tile(&mut self, z: i32, x: i32, y: i32) -> SqlResult<Vec<u8>> {
        let mut stmt = self.conn.prepare_cached("SELECT data FROM tiles WHERE z = ? AND x = ? AND y = ?")?;
        let data = stmt.query_row(params![z, x, y], |row| row.get(0))?;
        Ok(data)
    }

    /// Stat a tile - returns (exists, crc32)
    pub fn stat_tile(&mut self, z: i32, x: i32, y: i32) -> SqlResult<(bool, u32)> {
        let mut stmt = self.conn.prepare_cached("SELECT crc32 FROM tiles WHERE z = ? AND x = ? AND y = ?")?;
        match stmt.query_row(params![z, x, y], |row| {
            let crc: i64 = row.get(0)?;
            Ok(crc as u32)
        }) {
            Ok(crc) => Ok((true, crc)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok((false, 0)),
            Err(e) => Err(e),
        }
    }

    /// Set CRC for a tile
    pub fn set_crc(&mut self, z: i32, x: i32, y: i32, crc32: u32) -> SqlResult<()> {
        let mut stmt = self.conn.prepare_cached("UPDATE tiles SET crc32 = ? WHERE z = ? AND x = ? AND y = ?")?;
        stmt.execute(params![crc32 as i64, z, x, y])?;
        Ok(())
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
        
        if !self.read_only {
            // Checkpoint WAL
            self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;

            // Close and reopen to disable WAL
            drop(self.conn);
            let conn = Connection::open(&self.db_path)?;
            conn.execute_batch("PRAGMA journal_mode = DELETE")?;
            eprintln!("successfully reverted WAL after reopen");
        }
        Ok(())
    }
}