// A single file tile server reading from a pre-computed SQlite DB.
package main

import (
	"database/sql"
	"encoding/binary"
	"fmt"
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

type DatabaseManager struct {
	dbPool map[uint32]*sql.DB
	stmts  map[uint32]*sql.Stmt
}

func NewDatabaseManager() *DatabaseManager {
	return &DatabaseManager{
		dbPool: make(map[uint32]*sql.DB),
		stmts:  make(map[uint32]*sql.Stmt),
	}
}

// initializeWeekDatabases scans for database files and initializes connections
func (dbm *DatabaseManager) initializeWeekDatabases(folderPath string) error {
	files, err := os.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	dbCount := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		if !strings.HasPrefix(filename, "w") || !strings.HasSuffix(filename, ".db") {
			continue
		}

		// Extract version from filename (w1_*.db -> 1)
		name := strings.TrimPrefix(strings.TrimSuffix(filename, ".db"), "w")
		parts := strings.Split(name, "_")
		var version string
		if len(parts) < 2 {
			// invalid filename
			continue
		} else {
			version = parts[0]
		}
		_version, err := strconv.ParseUint(version, 10, 32)
		if err != nil {
			log.Printf("Invalid week database filename: %s", filename)
			continue
		}
		versionUint := uint32(_version)

		filename = folderPath + filename
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

		dbm.stmts[versionUint] = stmt
		dbm.dbPool[versionUint] = db
		dbCount++
	}

	if dbCount == 0 {
		return fmt.Errorf("no week database files found (looking for w*.db files)")
	}

	log.Printf("Initialized %d database(s)", dbCount)
	return nil
}

func (dbm *DatabaseManager) InitializeDatabases(folderPath string) error {
	err := dbm.initializeWeekDatabases(folderPath + "/weeks/")
	if err != nil {
		return err
	}

	return nil
}

func (dbm *DatabaseManager) GetTile(z, x, y int, version uint32) ([]byte, error) {
	stmt, exists := dbm.stmts[version]
	if !exists {
		return nil, fmt.Errorf("requested version %d not found", version)
	}

	var tileData []byte
	err := stmt.QueryRow(z, x, y).Scan(&tileData)
	return tileData, err
}

func (dbm *DatabaseManager) GetDateList() []uint32 {
	dates := make([]uint32, 0)
	for version, db := range dbm.dbPool {
		res, err := db.Query("SELECT date FROM versions")
		if err != nil {
			log.Printf("Error querying version for database %d: %v", version, err)
			continue
		}
		for res.Next() {
			var date uint32
			if err := res.Scan(&date); err != nil {
				log.Printf("Error scanning version for database %d: %v", version, err)
				continue
			}
			dates = append(dates, date)
		}
		defer res.Close()
	}
	sort.Slice(dates, func(i, j int) bool {
		return dates[i] < dates[j]
	})
	return dates
}

// GetAllDiffs retrieves all diffs for a given tile across all versions
func (dbm *DatabaseManager) GetAllDiffs(z, x, y int, writer http.ResponseWriter) error {
	versions := make([]uint32, 0, len(dbm.stmts))
	for version := range dbm.stmts {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[i] < versions[j]
	})
	for _, version := range versions {
		stmt, _ := dbm.stmts[version]
		var diffData []byte
		err := stmt.QueryRow(z, x, y).Scan(&diffData)
		if err != nil {
			log.Printf("Error querying diff for version %d: %v", version, err)
			continue
		}
		// Skip part with DateHours=0
		skip := 0
		if len(diffData) > 8 {
			dateHours := binary.LittleEndian.Uint32(diffData[:4])
			if dateHours == 0 {
				length := binary.LittleEndian.Uint32(diffData[4:8])
				skip = int(8 + length)
			}
		}
		_, err = writer.Write(diffData[skip:])
		if err != nil {
			log.Printf("Error writing diff for version %d: %v", version, err)
			return err
		}
	}
	return nil
}

// Close closes all database connections
func (dbm *DatabaseManager) Close() error {
	var lastErr error

	// Close prepared statements
	for version, stmt := range dbm.stmts {
		if err := stmt.Close(); err != nil {
			log.Printf("Error closing statement for version %s: %v", version, err)
			lastErr = err
		}
	}

	// Close database connections
	for version, db := range dbm.dbPool {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database for version %s: %v", version, err)
			lastErr = err
		}
	}

	return lastErr
}

func dateFromVersion(version float32) string {
	// Convert version float to date string (e.g., 1.001 -> 2025-01-01T01)
	// where 1 is the number of weeks since a base date (e.g., 2025-01-01)
	// and .001 is the number of hours into that week.
	baseDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	major := int(version)
	minor := int((version - float32(major)) * 1000)
	daysToAdd := major * 7
	hoursToAdd := minor

	targetDate := baseDate.AddDate(0, 0, daysToAdd).Add(time.Duration(hoursToAdd) * time.Hour)
	return targetDate.Format("2006-01-02T15")
}

type Asset struct {
	name string
	data []byte
	mime string
}

type TileServer struct {
	dataPath        string
	dbManager       *DatabaseManager
	diffPool        map[string]*sql.DB
	diffStmts       map[string]*sql.Stmt
	diffSortedDates []string
	indexHtml       string
	latestVersion   string
	previewImage    []byte
	faviconData     []byte
	assets          map[string]*Asset
}

func NewTileServer(dataPath string) (*TileServer, error) {
	ts := &TileServer{
		dataPath:        dataPath,
		diffPool:        make(map[string]*sql.DB),
		diffStmts:       make(map[string]*sql.Stmt),
		diffSortedDates: make([]string, 0),
		indexHtml:       "",
		assets:          make(map[string]*Asset),
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
	return ts, nil
}

// initializeDatabases scans for database files and initializes connections
func (ts *TileServer) initializeDatabases() error {
	ts.dbManager = NewDatabaseManager()
	err := ts.dbManager.InitializeDatabases(ts.dataPath)
	return err
}

func (ts *TileServer) initializeIndex() error {
	dates := ts.dbManager.GetDateList()
	ts.latestVersion = fmt.Sprintf("%.3f", dates[len(dates)-1])

	// load index.html.tmpl and replace $$VERSION_OPTIONS$$ with options
	data, err := os.ReadFile(ts.dataPath + "/index.html.tmpl")
	if err != nil {
		return fmt.Errorf("failed to read index.html.tmpl: %w", err)
	}
	content := string(data)

	options := make([]string, 0)
	for _, epochHour := range dates {
		desc := epochHourToDate(epochHour)
		value := fmt.Sprintf("{version: '%d', date: '%s'}", epochHour, desc)
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
	versionStr := vars["version"]
	zStr := vars["z"]
	xStr := vars["x"]
	yStr := vars["y"]

	z, x, y, err := ParseTileCoords(zStr, xStr, yStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	versionFloat, err := strconv.ParseFloat(versionStr, 32)
	if err != nil {
		http.Error(w, "invalid version", http.StatusBadRequest)
		return
	}
	versionUint := uint32(versionFloat)

	// Set appropriate headers
	tileKey := GetTileKey(z, x, y)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "public, max-age=86400") // Cache for 1 day
	w.Header().Set("ETag", fmt.Sprintf(`"%s-%s"`, versionUint, tileKey))

	// Check if client has cached version
	if match := r.Header.Get("If-None-Match"); match != "" {
		if match == fmt.Sprintf(`"%s-%s"`, versionUint, tileKey) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Write tile data
	data, err := ts.dbManager.GetTile(z, x, y, versionUint)
	if err == sql.ErrNoRows {
		http.Error(w, "tile not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("Database query error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// serveAllDiff handles all diff requests
func (ts *TileServer) serveAllDiff(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	zStr := vars["z"]
	xStr := vars["x"]
	yStr := vars["y"]
	from := strings.TrimSpace(r.URL.Query().Get("from"))
	to := strings.TrimSpace(r.URL.Query().Get("to"))

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
	ts.dbManager.GetAllDiffs(z, x, y, w)
}

func (ts *TileServer) serveIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(ts.indexHtml))
}

// Close closes all database connections
func (ts *TileServer) Close() error {
	return ts.dbManager.Close()
}

func (ts *TileServer) MakeLatestImage() ([]byte, error) {
	return nil, fmt.Errorf("TODO")

	// Get latest tile (z=0, x=0, y=0)
	// latestBaseVersion := strings.Split(ts.latestVersion, ".")[0]
	// latestTile, err := ts.GetTile(0, 0, 0, latestBaseVersion)
	// if err != nil {
	// 	return nil, err
	// }
	// latestImg, err := png.Decode(bytes.NewReader(latestTile))
	// if err != nil {
	// 	return nil, err
	// }

	// // Open basemap image
	// f, err := os.Open(path.Join(ts.dataPath, "osm000.png"))
	// if err != nil {
	// 	return latestTile, err
	// }
	// defer f.Close()
	// basemap, err := png.Decode(f)
	// if err != nil {
	// 	return latestTile, err
	// }

	// // Overlay latest tile on basemap
	// if basemap.Bounds() != latestImg.Bounds() {
	// 	return latestTile, fmt.Errorf("basemap size does not match latest tile size")
	// }
	// outImg := image.NewRGBA(basemap.Bounds())
	// for y := 0; y < basemap.Bounds().Dy(); y++ {
	// 	for x := 0; x < basemap.Bounds().Dx(); x++ {
	// 		r, g, b, a := basemap.At(x, y).RGBA()
	// 		tr, tg, tb, ta := latestImg.At(x, y).RGBA()
	// 		if ta > 0 {
	// 			outImg.Set(x, y, color.RGBA{uint8(tr >> 8), uint8(tg >> 8), uint8(tb >> 8), uint8(ta >> 8)})
	// 		} else {
	// 			outImg.Set(x, y, color.RGBA{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8), uint8(a >> 8)})
	// 		}
	// 	}
	// }

	// // Encode output image to PNG
	// var buf bytes.Buffer
	// if err := png.Encode(&buf, outImg); err != nil {
	// 	return latestTile, err
	// }

	// return buf.Bytes(), nil
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

func dateToEpochHour(date string) (uint32, error) {
	wplaceEpoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t, err := time.Parse("2006-01-02T15", date)
	if err != nil {
		return 0, err
	}
	return uint32(t.Sub(wplaceEpoch).Hours()), nil

}

func epochHourToDate(epochHour uint32) string {
	wplaceEpoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t := wplaceEpoch.Add(time.Duration(epochHour) * time.Hour)
	return t.Format("2006-01-02T15")
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
	r.HandleFunc("/tiles/{version:[0-9.]+}/{z:[0-9]+}/{x:[0-9]+}/{y:[0-9]+}.zst",
		tileServer.serveTile).Methods("GET")

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
