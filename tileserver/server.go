// A single file tile server reading from a pre-computed SQlite DB.
package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"image"
	"image/color"
	"image/png"
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

type TileServer struct {
	dataPath            string
	dbPool              map[string]*sql.DB
	stmts               map[string]*sql.Stmt
	versionDescriptions map[string]string
	indexHtml           string
	latestVersion       string
	previewImage        []byte
}

func NewTileServer(dataPath string) (*TileServer, error) {
	ts := &TileServer{
		dataPath:            dataPath,
		dbPool:              make(map[string]*sql.DB),
		stmts:               make(map[string]*sql.Stmt),
		versionDescriptions: make(map[string]string),
		indexHtml:           "",
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

// serveTile handles tile requests
func (ts *TileServer) serveTile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	version := vars["version"]
	zStr := vars["z"]
	xStr := vars["x"]
	yStr := vars["y"]

	// Parse coordinates
	z, err := strconv.Atoi(zStr)
	if err != nil {
		http.Error(w, "Invalid z coordinate", http.StatusBadRequest)
		return
	}

	x, err := strconv.Atoi(xStr)
	if err != nil {
		http.Error(w, "Invalid x coordinate", http.StatusBadRequest)
		return
	}

	y, err := strconv.Atoi(yStr)
	if err != nil {
		http.Error(w, "Invalid y coordinate", http.StatusBadRequest)
		return
	}

	// Validate coordinates (basic sanity check)
	if z < 0 || z > 11 || x < 0 || y < 0 || x >= (1<<z) || y >= (1<<z) {
		http.Error(w, "Invalid tile coordinates", http.StatusBadRequest)
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
	w.Header().Set("Content-Type", "image/png")
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
	r.HandleFunc("/tiles/{version:v[0-9a-z.]+}/{z:[0-9]+}/{x:[0-9]+}/{y:[0-9]+}.png",
		tileServer.serveTile).Methods("GET")

	// Root endpoint for index.html
	r.HandleFunc("/", tileServer.serveIndex).Methods("GET")

	// Preview image endpoint
	r.HandleFunc("/preview.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Length", strconv.Itoa(len(tileServer.previewImage)))
		w.WriteHeader(http.StatusOK)
		w.Write(tileServer.previewImage)
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
