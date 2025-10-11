package store

import (
	"database/sql"
	"fmt"
	hcrc "hash/crc32"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const defaultDbPath = "./tiles.db"

// Busy timeout for SQLite (in seconds)
const sqliteBusyTimeout = 20

type TileDB struct {
	DB       *sql.DB
	stmtPut  *sql.Stmt
	stmtGet  *sql.Stmt
	stmtStat *sql.Stmt
	stmtCrc  *sql.Stmt
	stmList  *sql.Stmt
}

func (db *TileDB) PutTile(z, x, y int, data []byte, crc32 uint32) error {
	return db.putWithRetry(z, x, y, data, crc32, 5)
}

func (db *TileDB) PutTileAutoCRC(z, x, y int, data []byte) error {
	crc32 := hcrc.ChecksumIEEE(data)
	return db.putWithRetry(z, x, y, data, crc32, 5)
}

func (db *TileDB) putWithRetry(z, x, y int, data []byte, crc32 uint32, retries int) error {
	for i := 0; i < retries; i++ {
		_, err := db.stmtPut.Exec(z, x, y, crc32, data)
		if err == nil {
			return nil
		}
		fmt.Printf("DB write error for tile (%d, %d, %d) (attempt %d/%d): %v\n", z, x, y, i+1, retries, err)
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("failed to write tile (%d, %d, %d) after %d attempts", z, x, y, retries)
}

func (db *TileDB) GetTile(z, x, y int) ([]byte, error) {
	row := db.stmtGet.QueryRow(z, x, y)
	var data []byte
	if err := row.Scan(&data); err != nil {
		return nil, fmt.Errorf("failed to get tile (%d, %d, %d): %w", z, x, y, err)
	}
	return data, nil
}

func (db *TileDB) StatTile(z, x, y int) (exists bool, crc uint32, err error) {
	row := db.stmtStat.QueryRow(z, x, y)
	if err := row.Scan(&crc); err != nil {
		if err == sql.ErrNoRows {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("failed to stat tile (%d, %d, %d): %w", z, x, y, err)
	}
	return true, crc, nil
}

func (db *TileDB) SetCRC(z, x, y int, crc32 uint32) error {
	_, err := db.stmtCrc.Exec(crc32, z, x, y)
	if err != nil {
		return err
	}
	return nil
}

// Worst case returns 4^11 tiles, ~16MiB. Acceptable
func (db *TileDB) ListTiles(z int) ([][2]uint16, error) {
	rows, err := db.stmList.Query(z)
	if err != nil {
		return nil, err
	}
	res := make([][2]uint16, 0)
	for rows.Next() {
		var x, y uint16
		if err := rows.Scan(&x, &y); err != nil {
			rows.Close()
			return nil, err
		}
		res = append(res, [2]uint16{x, y})
	}
	rows.Close()
	return res, nil
}

func (db *TileDB) init() error {
	var err error

	// Set busy timeout
	busyTimeoutMs := sqliteBusyTimeout * 1000
	_, err = db.DB.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeoutMs))
	if err != nil {
		return fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	// Enable WAL mode for better concurrency
	_, err = db.DB.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Set WAL journal size limit to 500MB
	if _, err := db.DB.Exec("PRAGMA journal_size_limit = 524288000"); err != nil {
		return fmt.Errorf("failed to set journal_size_limit: %w", err)
	}

	// Ensure schema
	_, err = db.DB.Exec(`CREATE TABLE IF NOT EXISTS tiles (
		z INTEGER NOT NULL,
		x INTEGER NOT NULL,
		y INTEGER NOT NULL,
		crc32 INTEGER,
		data BLOB NOT NULL,
		PRIMARY KEY (z, x, y)
	)`)
	if err != nil {
		return fmt.Errorf("failed to ensure schema: %w", err)
	}

	// Prepare statements
	return db.prepareStmt()
}

func (db *TileDB) prepareStmt() error {
	var err error
	db.stmtPut, err = db.DB.Prepare(`INSERT INTO tiles (z, x, y, crc32, data) VALUES (?, ?, ?, ?, ?) ON CONFLICT(z, x, y) DO UPDATE SET data=excluded.data,crc32=excluded.crc32`)
	if err != nil {
		return fmt.Errorf("failed to prepare put statement: %w", err)
	}
	db.stmtGet, err = db.DB.Prepare(`SELECT data FROM tiles WHERE z = ? AND x = ? AND y = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare get statement: %w", err)
	}
	db.stmtStat, err = db.DB.Prepare(`SELECT crc32 FROM tiles WHERE z = ? AND x = ? AND y = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare stat statement: %w", err)
	}
	db.stmtCrc, err = db.DB.Prepare(`UPDATE tiles SET crc32 = ? WHERE z = ? AND x = ? AND y = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare stat statement: %w", err)
	}
	db.stmList, err = db.DB.Prepare(`SELECT x, y FROM tiles WHERE z = ?`)
	if err != nil {
		return fmt.Errorf("failed to prepare stat statement: %w", err)
	}
	return nil
}

func (db *TileDB) Close() {
	_, err := db.DB.Exec("PRAGMA journal_mode = DELETE") // revert to the default behavior
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to revert journaling to non WAL: %v", err)
	}
	db.stmtPut.Close()
	db.stmtGet.Close()
	db.stmtStat.Close()
	db.DB.Close()
}

func NewTileDB(dbPath string) (TileDB, error) {
	if dbPath == "" {
		dbPath = defaultDbPath
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return TileDB{}, fmt.Errorf("failed to open database: %w", err)
	}
	tileDB := TileDB{DB: db}
	if err := tileDB.init(); err != nil {
		db.Close()
		return TileDB{}, err
	}
	return tileDB, nil
}
