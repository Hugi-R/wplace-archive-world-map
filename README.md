# wplace-archive-world-map

A Go-based project for ingesting, transforming, and serving [Wplace](https://wplace.live) archives.

This is what's behind [wplace.eralyon.net](https://wplace.eralyon.net/).

## Features

- Ingest and store map tiles from archives into an indexed SQLite DB.
- Incremental ingest, called `diff`, to save on storage.
- Merge and process tiles for various zoom levels.
- Serve tiles via an HTTP server using minimal CPU. [wplace.eralyon.net](https://wplace.eralyon.net/) run on a single core of an Intel Celeron J4005
- A simple map web page served by the tile server.

## Getting Started

### Prerequisites
- Go 1.24+
- Docker (optional, for containerizing the tile server)
- Wplace archive can be found at [Jazza-231/wplace-scripts](https://github.com/Jazza-231/wplace-scripts) or [murolem/wplace-archives](https://github.com/murolem/wplace-archives)

### Build & Run

```shell
./build.sh
# ls bin
# ingest  merger  tileserver
```

### Ingest
Ingest an archive into a DB. PNGs are converted to the palette used by this project.
**Currently only folder or 7z archives are supported.**

The files inside the archive should be like `*/X/Y.png` where X and Y are coordinates of the tile.

```shell
./bin/ingest --from wplace-archives/tiles-1_2025-08-29H18.7z --out data/tiles-1.db --workers 16
```

Ingest can build an incremental DB containing only the changed pixels compared to a base DB:
```shell
./bin/ingest --base data/tiles-1.db --from wplace-archives/tiles-2_2025-08-30H00.7z --out data/tiles-2.db --workers 16
```
This saves a lot of storage, and speeds up ingest when few tiles change. When many tiles change, ingest can be slower due to the extra compute required for diffs.

**KNOWN LIMITATION**: Unchanged pixels are encoded as transparent pixels. This means that if a pixel in Wplace changed from a color to transparent, that change is lost in the diff. This behavior simplifies applying diffs at runtime (in the browser) but is not an accurate archival format.

|     | tiles-1.db | tiles-2.db |
| --- | ---------- | ---------- |
| Size | 5.5 GB     | 385 MB     |
| Time | 35 m       | 16 m       |

(Ran on an AMD Ryzen 7 5700X3D)

### Merge
Create tiles for other zoom levels. Recursively merge and resize tiles (from level 10 to 0), keeping the majority pixel (ignoring transparent pixels).
This significantly increases the size of the DB.

```shell
./bin/merger --target data/tiles-1.db --workers 16 --initz 10
```

This also works on diff'ed DBs, where the base should have been merged first:
```shell
./bin/merger --base data/tiles-1.db --target data/tiles-2.db --workers 16 --initz 10
```

|     | tiles-1.db | tiles-2.db |
| --- | ---------- | ---------- |
| Size | 9.3 GB     | 695 MB     |
| Time | 16 m       | 7 m        |

(Ran on an AMD Ryzen 7 5700X3D)

### Tileserver
The tileserver looks for an `index.html.tmpl` and DB files named `vX_AAA.db`. DBs with `vX.Y` are increments from `vX`.
The folder used by the tileserver is configured with the `DATA_PATH` environment variable.

```shell
cp index.html data/index.html.tmpl
mv data/tiles-1.db data/v1_2025-08-29H18.db
mv data/tiles-2.db data/v1.01_2025-08-30H00.db
DATA_PATH=./data ./bin/tileserver
```
The server is available at `http://localhost:8080`.

## Disclaimer
- This is a cleaned-up version of a bunch of experiments. Documentation and tests are sparse, and will likely remain so.
- GenAI was used in parts of this project: for boilerplate Go code, and much of the HTML/CSS/JS.

## License

Code: MIT License.
Images (test): wplace.live and its community.
