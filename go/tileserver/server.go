// A single file tile server reading from a pre-computed SQlite DB.
package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

type Asset struct {
	name string
	data []byte
	mime string
}

type TileServer struct {
	dataPath            string
	dbPool              map[string]*sql.DB
	stmts               map[string]*sql.Stmt
	versionDescriptions map[string]string
	diffPool            map[string]*sql.DB
	diffStmts           map[string]*sql.Stmt
	diffSortedDates     []string
	indexHtml           string
	latestVersion       string
	previewImage        []byte
	faviconData         []byte
	assets              map[string]*Asset
}

func NewTileServer(dataPath string) (*TileServer, error) {
	ts := &TileServer{
		dataPath:            dataPath,
		dbPool:              make(map[string]*sql.DB),
		stmts:               make(map[string]*sql.Stmt),
		versionDescriptions: make(map[string]string),
		diffPool:            make(map[string]*sql.DB),
		diffStmts:           make(map[string]*sql.Stmt),
		diffSortedDates:     make([]string, 0),
		indexHtml:           "",
		assets:              make(map[string]*Asset),
	}

	if err := ts.initializeDatabases(); err != nil {
		return nil, err
	}
	if err := ts.initializeIndex(); err != nil {
		return nil, err
	}
	var err error
	ts.previewImage, err = ts.MakeLatestImage()
	if err != nil {
		fmt.Printf("Warning: failed to create preview image: %v\n", err)
	}
	ts.faviconData, err = ts.MakeFavicon()
	if err != nil {
		fmt.Printf("Warning: failed to load favicon: %v\n", err)
	}
	if err := ts.LoadAssets(); err != nil {
		return nil, err
	}
	if err := ts.initializeDiffs(); err != nil {
		fmt.Printf("Warning: failed to load diffs: %v\n", err)
	}
	return ts, nil
}

// initializeDatabases scans for database files and initializes connections
func (ts *TileServer) initializeDatabases() error {
	files, err := os.ReadDir(ts.dataPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	dbCount := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		if !strings.HasPrefix(filename, "v") || !strings.HasSuffix(filename, ".db") {
			continue
		}

		// Extract version from filename (v1_*.db -> 1, desc)
		name := strings.TrimSuffix(filename, ".db")
		parts := strings.Split(name, "_")
		var version string
		var description string
		if len(parts) == 2 {
			version = parts[0]
			description = parts[1]
		} else {
			version = parts[0]
			description = ""
		}
		ts.versionDescriptions[version] = description

		filename = ts.dataPath + "/" + filename
		log.Printf("Initializing database: %s (version %s)", filename, version)

		db, err := sql.Open("sqlite3", filename+"?cache=shared&mode=ro")
		if err != nil {
			return fmt.Errorf("failed to open database %s: %w", filename, err)
		}

		// Configure connection pool
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(3)
		db.SetConnMaxLifetime(24 * time.Hour) // Once a day refresh connections

		// Test the connection
		if err := db.Ping(); err != nil {
			db.Close()
			return fmt.Errorf("failed to ping database %s: %w", filename, err)
		}

		// Prepare the statement for this database
		stmt, err := db.Prepare("SELECT data FROM tiles WHERE z = ? AND x = ? AND y = ?")
		if err != nil {
			db.Close()
			return fmt.Errorf("failed to prepare statement for %s: %w", filename, err)
		}

		ts.stmts[version] = stmt
		ts.dbPool[version] = db
		dbCount++
	}

	if dbCount == 0 {
		return fmt.Errorf("no database files found (looking for v*.db files)")
	}

	log.Printf("Initialized %d database(s)", dbCount)
	return nil
}

func (ts *TileServer) initializeIndex() error {
	// Collect versions and sort numerically
	versions := make([]string, 0, len(ts.versionDescriptions))
	for v := range ts.versionDescriptions {
		versions = append(versions, v)
	}
	// Sort by numeric value after "v"
	sort.Slice(versions, func(i, j int) bool {
		vi := strings.TrimPrefix(versions[i], "v")
		vj := strings.TrimPrefix(versions[j], "v")
		// Try float comparison, fallback to string
		fi, erri := strconv.ParseFloat(vi, 64)
		fj, errj := strconv.ParseFloat(vj, 64)
		if erri == nil && errj == nil {
			return fi < fj
		}
		return vi < vj
	})
	ts.latestVersion = versions[len(versions)-1]

	// load index.html.tmpl and replace $$VERSION_OPTIONS$$ with options
	data, err := os.ReadFile(ts.dataPath + "/index.html.tmpl")
	if err != nil {
		return fmt.Errorf("failed to read index.html.tmpl: %w", err)
	}
	content := string(data)

	options := make([]string, 0)
	for _, version := range versions {
		desc := ts.versionDescriptions[version]
		value := fmt.Sprintf("{version: '%s', date: '%s'}", version, desc)
		options = append(options, value)
	}
	content = strings.ReplaceAll(content, "//$$VERSION_OPTIONS$$", strings.Join(options, ","))
	ts.indexHtml = content
	return nil
}

// GetTileKey generates the key for a tile based on z/x/y coordinates
func GetTileKey(z, x, y int) string {
	return fmt.Sprintf("%d/%d/%d", z, x, y)
}

// ParseTileCoords parse and check tile coordinates
func ParseTileCoords(zStr, xStr, yStr string) (z, x, y int, err error) {
	z, err = strconv.Atoi(zStr)
	if err != nil {
		err = fmt.Errorf("invalid z coordinate")
		return
	}

	x, err = strconv.Atoi(xStr)
	if err != nil {
		err = fmt.Errorf("invalid x coordinate")
		return
	}

	y, err = strconv.Atoi(yStr)
	if err != nil {
		err = fmt.Errorf("invalid y coordinate")
		return
	}

	// Validate coordinates (basic sanity check)
	if z < 0 || z > 11 || x < 0 || y < 0 || x >= (1<<z) || y >= (1<<z) {
		err = fmt.Errorf("tile coordinate out of bound")
		return
	}

	return
}

// serveTile handles tile requests
func (ts *TileServer) serveTile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	version := vars["version"]
	zStr := vars["z"]
	xStr := vars["x"]
	yStr := vars["y"]

	z, x, y, err := ParseTileCoords(zStr, xStr, yStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tileData, err := ts.GetTile(z, x, y, version)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		log.Printf("Database query error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Set appropriate headers
	tileKey := GetTileKey(z, x, y)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(tileData)))
	w.Header().Set("Cache-Control", "public, max-age=86400") // Cache for 1 day
	w.Header().Set("ETag", fmt.Sprintf(`"%s-%s"`, version, tileKey))

	// Check if client has cached version
	if match := r.Header.Get("If-None-Match"); match != "" {
		if match == fmt.Sprintf(`"%s-%s"`, version, tileKey) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Write tile data
	w.WriteHeader(http.StatusOK)
	w.Write(tileData)
}

func (ts *TileServer) GetTile(z, x, y int, version string) ([]byte, error) {
	stmt, exists := ts.stmts[version]
	if !exists {
		return nil, fmt.Errorf("requested version %s not found", version)
	}

	var tileData []byte
	err := stmt.QueryRow(z, x, y).Scan(&tileData)
	return tileData, err
}

func (ts *TileServer) serveIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(ts.indexHtml))
}

// Close closes all database connections
func (ts *TileServer) Close() error {
	var lastErr error

	// Close prepared statements
	for version, stmt := range ts.stmts {
		if err := stmt.Close(); err != nil {
			log.Printf("Error closing statement for version %s: %v", version, err)
			lastErr = err
		}
	}

	// Close database connections
	for version, db := range ts.dbPool {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database for version %s: %v", version, err)
			lastErr = err
		}
	}

	return lastErr
}

func (ts *TileServer) MakeLatestImage() ([]byte, error) {
	// Get latest tile (z=0, x=0, y=0)
	latestBaseVersion := strings.Split(ts.latestVersion, ".")[0]
	latestTile, err := ts.GetTile(0, 0, 0, latestBaseVersion)
	if err != nil {
		return nil, err
	}
	latestImg, err := png.Decode(bytes.NewReader(latestTile))
	if err != nil {
		return nil, err
	}

	// Open basemap image
	f, err := os.Open(path.Join(ts.dataPath, "osm000.png"))
	if err != nil {
		return latestTile, err
	}
	defer f.Close()
	basemap, err := png.Decode(f)
	if err != nil {
		return latestTile, err
	}

	// Overlay latest tile on basemap
	if basemap.Bounds() != latestImg.Bounds() {
		return latestTile, fmt.Errorf("basemap size does not match latest tile size")
	}
	outImg := image.NewRGBA(basemap.Bounds())
	for y := 0; y < basemap.Bounds().Dy(); y++ {
		for x := 0; x < basemap.Bounds().Dx(); x++ {
			r, g, b, a := basemap.At(x, y).RGBA()
			tr, tg, tb, ta := latestImg.At(x, y).RGBA()
			if ta > 0 {
				outImg.Set(x, y, color.RGBA{uint8(tr >> 8), uint8(tg >> 8), uint8(tb >> 8), uint8(ta >> 8)})
			} else {
				outImg.Set(x, y, color.RGBA{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8), uint8(a >> 8)})
			}
		}
	}

	// Encode output image to PNG
	var buf bytes.Buffer
	if err := png.Encode(&buf, outImg); err != nil {
		return latestTile, err
	}

	return buf.Bytes(), nil
}

func (ts *TileServer) MakeFavicon() ([]byte, error) {
	f, err := os.Open(path.Join(ts.dataPath, "favicon.ico"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	faviconData, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return faviconData, nil
}

// LoadAssets loads static assets from the assets directory
func (ts *TileServer) LoadAssets() error {
	assetsFolder := path.Join(ts.dataPath, "assets")
	files, err := os.ReadDir(assetsFolder)
	if err != nil {
		return fmt.Errorf("failed to read assets directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		data, err := os.ReadFile(path.Join(assetsFolder, filename))
		if err != nil {
			return fmt.Errorf("failed to read asset %s: %w", filename, err)
		}
		ts.assets[filename] = &Asset{name: filename, data: data, mime: getMimeType(filename)}
		log.Printf("Loaded asset: %s (%d bytes)", filename, len(data))
	}

	return nil
}

func getMimeType(filename string) string {
	ext := strings.ToLower(path.Ext(filename))
	switch ext {
	case ".js":
		return "application/javascript"
	case ".css":
		return "text/css"
	case ".html":
		return "text/html"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".svg":
		return "image/svg+xml"
	case ".wasm":
		return "application/wasm"
	default:
		return "application/octet-stream"
	}
}

// initializeDiffs scans for database files and initializes connections
func (ts *TileServer) initializeDiffs() error {
	files, err := os.ReadDir(path.Join(ts.dataPath, "diffs"))
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	dbCount := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		if !strings.HasPrefix(filename, "diff_") || !strings.HasSuffix(filename, ".db") {
			continue
		}

		// Extract date from filename (diff_v32.001_2025-01-01T01.db -> 2025-01-01T01)
		name := strings.TrimPrefix(strings.TrimSuffix(filename, ".db"), "diff_")
		parts := strings.Split(name, "_")
		if len(parts) != 2 {
			continue
		}
		date := parts[1]

		filename = path.Join(ts.dataPath, "diffs", filename)
		log.Printf("Initializing diff: %s (date %s)", filename, date)

		db, err := sql.Open("sqlite3", filename+"?cache=shared&mode=ro")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open diff database %s: %w", filename, err)
			continue
		}

		// Configure connection pool
		db.SetMaxOpenConns(3)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(24 * time.Hour) // Once a day refresh connections

		// Test the connection
		if err := db.Ping(); err != nil {
			db.Close()
			fmt.Fprintf(os.Stderr, "failed to ping diff database %s: %w", filename, err)
			continue
		}

		// Prepare the statement for this database
		stmt, err := db.Prepare("SELECT data FROM tiles WHERE z = 11 AND x = ? AND y = ?")
		if err != nil {
			db.Close()
			fmt.Fprintf(os.Stderr, "failed to prepare statement for %s: %w", filename, err)
			continue
		}

		ts.diffStmts[date] = stmt
		ts.diffPool[date] = db
		ts.diffSortedDates = append(ts.diffSortedDates, date)
		dbCount++
	}
	// Sort diff dates
	sort.Strings(ts.diffSortedDates)

	log.Printf("Initialized %d diff database(s)", dbCount)
	return nil
}

// GetDiffList return json list of diff dates
func (ts *TileServer) GetDiffList() string {
	json := "["
	for _, date := range ts.diffSortedDates {
		json += "\"" + date + "\","
	}
	json = json[0 : len(json)-1]
	json += "]"
	return json
}

// serveDiff handles diff tile requests
func (ts *TileServer) serveDiff(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	date := vars["date"]
	zStr := vars["z"]
	xStr := vars["x"]
	yStr := vars["y"]

	z, x, y, err := ParseTileCoords(zStr, xStr, yStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if z != 11 {
		http.Error(w, "diff tiles only supported for z=11", http.StatusBadRequest)
		return
	}

	tileData, err := ts.GetDiff(x, y, date)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		log.Printf("Database query error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Set appropriate headers
	etag := fmt.Sprintf(`"diff-%s-%s"`, date, GetTileKey(z, x, y))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(tileData)))
	w.Header().Set("Cache-Control", "public, max-age=86400") // Cache for 1 day
	w.Header().Set("ETag", etag)
	// Check if client has cached version
	if match := r.Header.Get("If-None-Match"); match != "" {
		if match == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Write tile data
	w.WriteHeader(http.StatusOK)
	w.Write(tileData)
}

func (ts *TileServer) GetDiff(x, y int, date string) ([]byte, error) {
	stmt, exists := ts.diffStmts[date]
	if !exists {
		return nil, fmt.Errorf("requested version %s not found", date)
	}

	var tileData []byte
	err := stmt.QueryRow(x, y).Scan(&tileData)
	return tileData, err
}

// serveDiff handles diff tile requests
func (ts *TileServer) serveAllDiff(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	zStr := vars["z"]
	xStr := vars["x"]
	yStr := vars["y"]
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	z, x, y, err := ParseTileCoords(zStr, xStr, yStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if z != 11 {
		http.Error(w, "diff tiles only supported for z=11", http.StatusBadRequest)
		return
	}

	// Set appropriate headers
	etag := fmt.Sprintf(`"alldiff-%s-%s-from%d-to%s"`, GetTileKey(z, x, y), from, to)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "public, max-age=3600") // Cache for 1 hour
	w.Header().Set("ETag", etag)
	// Check if client has cached version
	if match := r.Header.Get("If-None-Match"); match != "" {
		if match == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Write tile data
	w.WriteHeader(http.StatusOK)
	ts.GetAllDiff(x, y, w, from, to)

}

func (ts *TileServer) GetAllDiff(x, y int, writer http.ResponseWriter, from, to string) error {
	dateFrom, err := dateToEpochHour(from)
	if err != nil {
		dateFrom = 0
	}
	dateTo, err := dateToEpochHour(to)
	if err != nil {
		dateTo = 2 ^ 32 - 1
	}
	for _, dateStr := range ts.diffSortedDates {
		date, err := dateToEpochHour(dateStr)
		if err != nil {
			return err
		}
		if date < dateFrom {
			continue
		}
		if date > dateTo {
			continue
		}

		stmt, exists := ts.diffStmts[dateStr]
		if !exists {
			continue
		}
		var tileData []byte
		err = stmt.QueryRow(x, y).Scan(&tileData)
		if err == nil {
			// write date as number of hours since Unix epoch
			dateBytes := make([]byte, 4)
			hours, err := dateToEpochHour(dateStr)
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(dateBytes, hours)
			_, err = writer.Write(dateBytes)
			if err != nil {
				return err
			}

			// Write size as 4-byte little-endian
			size := len(tileData)
			sizeBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(sizeBytes, uint32(size))
			_, err = writer.Write(sizeBytes)
			if err != nil {
				return err
			}

			_, err = writer.Write(tileData)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func dateToEpochHour(date string) (uint32, error) {
	t, err := time.Parse("2006-01-02T15", date)
	if err != nil {
		return 0, err
	}
	return uint32(t.Unix() / 3600), nil

}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	dataPath := os.Getenv("DATA_PATH")
	if dataPath == "" {
		dataPath = "."
	}

	tileServer, err := NewTileServer(dataPath)
	if err != nil {
		log.Fatalf("Failed to create tile server: %v", err)
	}
	defer tileServer.Close()

	r := mux.NewRouter()

	// Tile endpoint with version, z, x, y parameters
	r.HandleFunc("/tiles/{version:v[0-9a-z.]+}/{z:[0-9]+}/{x:[0-9]+}/{y:[0-9]+}.zst",
		tileServer.serveTile).Methods("GET")

	r.HandleFunc("/diff", func(w http.ResponseWriter, r *http.Request) {
		diffList := tileServer.GetDiffList()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(diffList))
	}).Methods("GET")

	// Diff endpoint with date, z, x, y parameters
	// eg: /diff/2025-01-01T01/11/0/0.zst
	// Note that diff only suport z=11
	r.HandleFunc("/diff/{date:[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}}/{z:[0-9]+}/{x:[0-9]+}/{y:[0-9]+}.zst",
		tileServer.serveDiff).Methods("GET")

	// All diffs endpoint with z, x, y parameters
	// eg: /diff/all/11/0/0.zst
	// Note that diff only suport z=11
	r.HandleFunc("/diff/all/{z:[0-9]+}/{x:[0-9]+}/{y:[0-9]+}.zst",
		tileServer.serveAllDiff).Methods("GET")

	// Root endpoint for index.html
	r.HandleFunc("/", tileServer.serveIndex).Methods("GET")

	// Preview image endpoint
	r.HandleFunc("/preview.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Length", strconv.Itoa(len(tileServer.previewImage)))
		w.WriteHeader(http.StatusOK)
		w.Write(tileServer.previewImage)
	}).Methods("GET")

	// Favicon endpoint
	r.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/x-icon")
		w.Header().Set("Content-Length", strconv.Itoa(len(tileServer.faviconData)))
		w.WriteHeader(http.StatusOK)
		w.Write(tileServer.faviconData)
	}).Methods("GET")

	// Assets endpoint
	r.HandleFunc("/assets/{filename}", func(w http.ResponseWriter, r *http.Request) {
		filename := mux.Vars(r)["filename"]
		if asset, exists := tileServer.assets[filename]; exists {
			w.Header().Set("Content-Type", asset.mime)
			w.Header().Set("Content-Length", strconv.Itoa(len(asset.data)))
			w.WriteHeader(http.StatusOK)
			w.Write(asset.data)
		} else {
			http.NotFound(w, r)
		}
	}).Methods("GET")

	// Add middleware for logging
	r.Use(loggingMiddleware)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("Starting tile server on :8080")
	log.Fatal(server.ListenAndServe())
}

// loggingMiddleware logs HTTP requests
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Create a response writer wrapper to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		log.Printf("%s %s %d %v", r.Method, r.URL.Path,
			wrapped.statusCode, time.Since(start))
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
