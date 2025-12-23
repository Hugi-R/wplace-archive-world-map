use rusqlite::{Connection, params, Result as SqlResult};
use std::time::Duration;

pub const DEFAULT_DB_PATH: &str = "./tiles.db";
pub const SQLITE_BUSY_TIMEOUT_SECS: u64 = 20;

pub struct TileDB {
    db_path: String,
    read_only: bool,
    conn: Connection,
}

impl TileDB {
    /// Create a new TileDB instance
    pub fn new(db_path: impl AsRef<str>, read_only: bool) -> SqlResult<Self> {
        let db_path_str = db_path.as_ref();
        let path = if db_path_str.is_empty() {
            DEFAULT_DB_PATH
        } else {
            db_path_str
        };

        let conn = if read_only {
            Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?
        } else {
            Connection::open(path)?
        };

        let mut db = TileDB {
            db_path: path.to_string(),
            read_only,
            conn,
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

        self.prepare_statements()?;
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

    fn prepare_statements(&mut self) -> SqlResult<()> {
        // Statements are prepared on-demand in methods
        Ok(())
    }

    /// Put a tile into the database with retry logic
    pub fn put_tile(&mut self, z: i32, x: i32, y: i32, data: &[u8], crc32: u32) -> SqlResult<()> {
        self.put_with_retry(z, x, y, data, crc32, 5)
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
            match self.conn.execute(
                "INSERT INTO tiles (z, x, y, crc32, data) VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(z, x, y) DO UPDATE SET data=excluded.data,crc32=excluded.crc32",
                params![z, x, y, crc32 as i64, data],
            ) {
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
    pub fn get_tile(&self, z: i32, x: i32, y: i32) -> SqlResult<Vec<u8>> {
        let mut stmt = self.conn.prepare("SELECT data FROM tiles WHERE z = ? AND x = ? AND y = ?")?;
        let data = stmt.query_row(params![z, x, y], |row| row.get(0))?;
        Ok(data)
    }

    /// Stat a tile - returns (exists, crc32)
    pub fn stat_tile(&self, z: i32, x: i32, y: i32) -> SqlResult<(bool, u32)> {
        let mut stmt = self.conn.prepare("SELECT crc32 FROM tiles WHERE z = ? AND x = ? AND y = ?")?;
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
        self.conn.execute(
            "UPDATE tiles SET crc32 = ? WHERE z = ? AND x = ? AND y = ?",
            params![crc32 as i64, z, x, y],
        )?;
        Ok(())
    }

    /// List tiles at zoom level z
    pub fn list_tiles(&self, z: i32) -> SqlResult<Vec<(u16, u16)>> {
        let mut stmt = self.conn.prepare("SELECT x, y FROM tiles WHERE z = ?")?;
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
