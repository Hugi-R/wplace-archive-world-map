package store

import (
	"database/sql"
	"hash/crc32"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type ReaderSqlite struct {
	db    *sql.DB
	rows  *sql.Rows
	state int // 0=closed, 1=open, 2=querying, 3=done
}

func (rs *ReaderSqlite) Open(dbPath string) error {
	if rs.state != 0 {
		return fmt.Errorf("already open")
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	rs.db = db
	rs.state = 1
	return nil
}

func (rs *ReaderSqlite) Close() error {
	if rs.rows != nil {
		rs.rows.Close()
		rs.rows = nil
	}
	if rs.db != nil {
		_ = rs.db.Close()
		rs.db = nil
	}
	rs.state = 0
	return nil
}

func (rs *ReaderSqlite) ReadOne() (Job, bool, error) {
	if rs.state == 0 {
		return Job{}, false, fmt.Errorf("not open")
	}

	if rs.state == 1 {
		rows, err := rs.db.Query("SELECT x, y, data FROM tiles ORDER BY x, y")
		if err != nil {
			return Job{}, false, fmt.Errorf("failed to query tiles: %w", err)
		}
		rs.rows = rows
		rs.state = 2
	}

	if rs.state == 3 {
		return Job{}, false, nil
	}

	for rs.rows.Next() {
		var x, y int
		var data []byte
		if err := rs.rows.Scan(&x, &y, &data); err != nil {
			return Job{}, false, fmt.Errorf("failed to scan row: %w", err)
		}
		crc := crc32.ChecksumIEEE(data)
		return Job{Z: 11, X: x, Y: y, Data: data, Crc32: crc}, true, nil
	}

	if err := rs.rows.Err(); err != nil {
		return Job{}, false, err
	}

	rs.rows.Close()
	rs.rows = nil
	rs.state = 3
	return Job{}, false, nil
}

func (rs *ReaderSqlite) ReadNextGood() (Job, bool, error) {
	j, ok, err := rs.ReadOne()
	if !ok {
		return j, ok, err
	}
	if err != nil {
		return rs.ReadNextGood()
	}
	return j, ok, err
}
