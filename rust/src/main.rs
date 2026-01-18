use std::error::Error;
use std::io::{Read, Write, Cursor};
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::time::Instant;

use crate::tilesdb::TileDB;

mod image;
mod palette;
mod tilesdb;
mod ingest;
mod reader_targz;
mod merge;
mod screenshot;
mod ingest_diff;

fn usage(program: &str) {
    eprintln!("Usage: {} <command> [args]", program);
    eprintln!("Commands:");
    eprintln!("  compress <input.png> <out.pal.zst>");
    eprintln!("  decompress <in.pal.zst> <out.png>");
    eprintln!("  roundtrip            # use PNGs in src/testdata and write results to target/roundtrip/");
    eprintln!("  benchmark            # compress all PNGs in src/testdata, report total time and compressed size");
    eprintln!("  ingest <input> <output> [base] [workers]  # ingest tiles from tar.gz into database");
}

fn cmd_compress(input: &Path, out: &Path) -> Result<(), Box<dyn Error>> {
    image::png_file_to_compressed_paletted(input, out)
}

fn cmd_decompress(input: &Path, out: &Path) -> Result<(), Box<dyn Error>> {
    image::compressed_paletted_to_png(input, out)
}

fn cmd_roundtrip() -> Result<(), Box<dyn Error>> {
    let testdir = Path::new("src/testdata");
    if !testdir.exists() {
        eprintln!("testdata directory '{}' not found", testdir.display());
        return Ok(());
    }

    let outdir = Path::new("target/roundtrip");
    fs::create_dir_all(outdir)?;

    let mut found = 0;
    for entry in fs::read_dir(testdir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() { continue; }
        if let Some(ext) = path.extension() {
            if ext.eq_ignore_ascii_case("png") {
                found += 1;
                let basename = path.file_stem().and_then(|s| s.to_str()).unwrap_or("img");
                let compressed = outdir.join(format!("{}.pal.zst", basename));
                let out_png = outdir.join(format!("{}.out.png", basename));
                eprintln!("Processing {} -> {} -> {}", path.display(), compressed.display(), out_png.display());
                match cmd_compress(&path, &compressed) {
                    Ok(()) => match cmd_decompress(&compressed, &out_png) {
                        Ok(()) => eprintln!("OK: {}", basename),
                        Err(e) => eprintln!("Decompress error for {}: {}", basename, e),
                    },
                    Err(e) => eprintln!("Compress error for {}: {}", basename, e),
                }
            }
        }
    }

    if found == 0 {
        eprintln!("No PNG files found in {}", testdir.display());
    }
    Ok(())
}

fn cmd_benchmark() -> Result<(), Box<dyn Error>> {
    let testdir = Path::new("src/testdata");
    if !testdir.exists() {
        eprintln!("testdata directory '{}' not found", testdir.display());
        return Ok(());
    }

    let outdir = Path::new("target/benchmark");
    fs::create_dir_all(outdir)?;

    let mut total_bytes: u64 = 0;
    let mut files_processed: usize = 0;

    let start = Instant::now();
    for entry in fs::read_dir(testdir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() { continue; }
        if let Some(ext) = path.extension() {
            if ext.eq_ignore_ascii_case("png") {
                files_processed += 1;
                let basename = path.file_stem().and_then(|s| s.to_str()).unwrap_or("img");
                let compressed = outdir.join(format!("{}.pal.zst", basename));
                // compress; ignore per-file decompress step here
                image::png_file_to_compressed_paletted(&path, &compressed)?;
                let meta = fs::metadata(&compressed)?;
                total_bytes += meta.len();
            }
        }
    }
    let duration = start.elapsed();

    println!("Benchmark: processed {} files", files_processed);
    println!("Total compressed size: {} bytes", total_bytes);
    println!("Elapsed: {:.3} seconds", duration.as_secs_f64());
    Ok(())
}

fn cmd_benchmark_resize() -> Result<(), Box<dyn Error>> {
    let testdir = Path::new("src/testdata");
    if !testdir.exists() {
        eprintln!("testdata directory '{}' not found", testdir.display());
        return Ok(());
    }

    let outdir = Path::new("target/benchmark-resize");
    fs::create_dir_all(outdir)?;

    let mut total_bytes: u64 = 0;
    let mut files_processed: usize = 0;

    let start = Instant::now();
    for entry in fs::read_dir(testdir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() { continue; }
        if let Some(ext) = path.extension() {
            if ext.eq_ignore_ascii_case("png") {
                files_processed += 1;
                let basename = path.file_stem().and_then(|s| s.to_str()).unwrap_or("img");
                let compressed = outdir.join(format!("{}.pal.zst", basename));

                let img = image::png_file_to_paletted(&path)?;
                let mut weights = [100u32; 256];
                weights[0] = 0; // don't care about transparent pixels
                weights[1] = 50; // reduce importance of black pixels
                let block_size = 2;
                let resized = image::downscale_mode_weighted(&img.indices, img.width, img.height, &weights, block_size);
                let resized_img = image::PalettedImage {
                    width: img.width / block_size,
                    height: img.height / block_size,
                    indices: resized,
                };

                let compressed_img = image::paletted_to_compressed_bytes(&resized_img)?;
                let mut of = File::create(&compressed)?;
                of.write_all(&compressed_img)?;

                let meta = fs::metadata(&compressed)?;
                total_bytes += meta.len();
            }
        }
    }
    let duration = start.elapsed();

    println!("Benchmark: processed {} files", files_processed);
    println!("Total compressed size: {} bytes", total_bytes);
    println!("Elapsed: {:.3} seconds", duration.as_secs_f64());
    Ok(())
}

fn cmd_4to1(in1: &Path, in2: &Path, in3: &Path, in4: &Path, out: &Path) -> Result<(), Box<dyn Error>> {
    let p1 = image::png_file_to_paletted(in1)?;
    let p2 = image::png_file_to_paletted(in2)?;
    let p3 = image::png_file_to_paletted(in3)?;
    let p4 = image::png_file_to_paletted(in4)?;

    let mut weights = [100u32; 256];
    weights[0] = 0; // don't care about transparent pixels
    weights[1] = 50; // reduce importance of black pixels
    let start = Instant::now();
    let res = image::downscale_4to1(&p1, &p2, &p3, &p4, &weights);
    let duration = start.elapsed();
    println!("Downscale 4to1 took {:.3} seconds", duration.as_secs_f64());

    image::paletted_to_png_file(&res, out)?;

    Ok(())
}

fn cmd_merge(input: &str, base: &str, workers: usize) -> Result<(), Box<dyn Error>> {
    if base == "" {
        merge::merge(input, workers)
    } else {
        merge::merge_diff(input, base, workers)
    }
}

fn cmd_diff(base: &str, new: &str, out: &str) -> Result<(), Box<dyn Error>> {
    let base_img = image::png_file_to_paletted(&Path::new(base))?;
    let new_img = image::png_file_to_paletted(&Path::new(new))?;
    let (_, diff) = image::diff_paletted(&base_img, &new_img);
    image::paletted_to_png_file(&diff, Path::new(out))?;

    Ok(())
}

fn cmd_undiff(base: &str, diff: &str, out: &str) -> Result<(), Box<dyn Error>> {
    let base_img = image::png_file_to_paletted(&Path::new(base))?;
    let diff_img = image::png_file_to_paletted(&Path::new(diff))?;
    let undiff = image::apply_diff_paletted(&base_img, &diff_img);
    image::paletted_to_png_file(&undiff, Path::new(out))?;

    Ok(())
}

fn cmd_screenshot(out: &str, base_url: &str, version: &str, x1: i64, y1: i64, x2: i64, y2: i64) -> Result<(), Box<dyn Error>> {
    let img = screenshot::native_screenshot(base_url, version, x1, y1, x2, y2)?;
    image::paletted_to_png_file(&img, &Path::new(out))
}

fn cmd_zst_bench(data_db: &str) -> anyhow::Result<()> {
    let mut db = TileDB::new(Path::new(data_db), true)?;
    
    fn decompress(compressed: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut dec = zstd::Decoder::new(Cursor::new(compressed))?;
        let mut decompressed = Vec::new();
        dec.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    fn compress(data: &[u8], level: i32) -> anyhow::Result<Vec<u8>> {
        let mut enc = zstd::Encoder::new(Vec::new(), level)?;
        enc.write_all(data)?;
        let compressed = enc.finish()?;
        Ok(compressed)
    }

    fn avg(x: Vec<u128>) -> f64 {
        let sum: f64 = x.iter().map(|&v| v as f64).sum();
        sum / (x.len() as f64)
    }

    fn stdev(x: Vec<u128>, avg: f64) -> f64 {
        let sum: f64 = x.iter().map(|&v| (v as f64 - avg)*(v as f64 - avg)).sum();
        sum/(x.len() - 1) as f64
    }

    // header
    println!("type,level,total,avg,stdev");

    for level in 1..19 {
        let mut comp_times = Vec::new();
        let mut decomp_times = Vec::new();
        let mut sizes = Vec::new();


        let tiles = db.list_tiles(11)?;
        for (x, y) in tiles {
            let data = db.get_tile(11, x as i32, y as i32)?;
            let data = decompress(&data)?;
            let t =  std::time::Instant::now();
            let comp = compress(&data, level)?;
            let elapsed = t.elapsed().as_nanos();
            comp_times.push(elapsed);
            sizes.push(comp.len() as u128);
            let t =  std::time::Instant::now();
            let _ = decompress(&comp)?;
            let elapsed = t.elapsed().as_nanos();
            decomp_times.push(elapsed);
        }

        let total: u128 = comp_times.iter().sum();
        let a = avg(comp_times.clone());
        let d = stdev(comp_times.clone(), a);
        println!("compression,{level},{total},{a},{d}");

        let total: u128 = decomp_times.iter().sum();
        let a = avg(decomp_times.clone());
        let d = stdev(decomp_times.clone(), a);
        println!("decompression,{level},{total},{a},{d}");

        let total: u128 = sizes.iter().sum();
        let a = avg(sizes.clone());
        let d = stdev(sizes.clone(), a);
        println!("size,{level},{total},{a},{d}");
    }
    Ok(())
}

fn cmd_diffconvert(out_folder: &str, diff_folder: &str, workers: usize) -> anyhow::Result<()> {
    ingest_diff::convert(out_folder, diff_folder, workers)
}

fn cmd_apgn(in_folder: &str, out_file: &str) -> anyhow::Result<()> {
    let mut frames = Vec::new();
    for entry in std::fs::read_dir(in_folder)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "png") {
            frames.push(image::png_file_to_paletted(&path)?);
        }
    }
    image::paletted_to_apng(frames, std::fs::File::create(out_file)?, 200)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage(&args[0]);
        std::process::exit(1);
    }
    match args[1].as_str() {
        "compress" => {
            if args.len() != 4 { usage(&args[0]); std::process::exit(2); }
            let input = PathBuf::from(&args[2]);
            let out = PathBuf::from(&args[3]);
            if let Err(e) = cmd_compress(&input, &out) {
                eprintln!("compress failed: {}", e);
                std::process::exit(3);
            }
        }
        "decompress" => {
            if args.len() != 4 { usage(&args[0]); std::process::exit(2); }
            let input = PathBuf::from(&args[2]);
            let out = PathBuf::from(&args[3]);
            if let Err(e) = cmd_decompress(&input, &out) {
                eprintln!("decompress failed: {}", e);
                std::process::exit(4);
            }
        }
        "roundtrip" => {
            if let Err(e) = cmd_roundtrip() {
                eprintln!("roundtrip failed: {}", e);
                std::process::exit(5);
            }
        }
        "benchmark" => {
            if let Err(e) = cmd_benchmark() {
                eprintln!("benchmark failed: {}", e);
                std::process::exit(6);
            }
        }
        "benchmark-resize" => {
            if let Err(e) = cmd_benchmark_resize() {
                eprintln!("benchmark failed: {}", e);
                std::process::exit(6);
            }
        }
        "ingest" => {
            if args.len() < 4 { usage(&args[0]); std::process::exit(2); }
            let input = &args[2];
            let output = &args[3];
            let base = if args.len() > 4 { args[4].as_str() } else { "" };
            let workers = if args.len() > 5 {
                args[5].parse::<usize>().unwrap_or(1)
            } else {
                10
            };
            if let Err(e) = ingest::ingest(input, output, base, workers) {
                eprintln!("ingest failed: {}", e);
                std::process::exit(7);
            }
        }
        "4to1" => {
            if args.len() != 7 { usage(&args[0]); std::process::exit(2); }
            let in1 = PathBuf::from(&args[2]);
            let in2 = PathBuf::from(&args[3]);
            let in3 = PathBuf::from(&args[4]);
            let in4 = PathBuf::from(&args[5]);
            let out = PathBuf::from(&args[6]);
            if let Err(e) = cmd_4to1(&in1, &in2, &in3, &in4, &out) {
                eprintln!("4to1 failed: {}", e);
                std::process::exit(8);
            }
        }
        "merge" => {
            if args.len() < 2 { usage(&args[0]); std::process::exit(2); }
            let input = &args[2];
            let base = if args.len() > 3 { args[3].as_str() } else { "" };
            let workers = if args.len() > 4 {
                args[4].parse::<usize>().unwrap_or(1)
            } else {
                10
            };
            if let Err(e) = cmd_merge(input, base, workers) {
                eprintln!("merge failed: {}", e);
                std::process::exit(9);
            }
        }
        "diff" => {
            if args.len() < 4 { usage(&args[0]); std::process::exit(2); }
            let base = &args[2];
            let new= &args[3];
            let out = &args[4];
            if let Err(e) = cmd_diff(base, new, out) {
                eprintln!("diff failed: {}", e);
                std::process::exit(10);
            }
        }
        "undiff" => {
            if args.len() < 4 { usage(&args[0]); std::process::exit(2); }
            let base = &args[2];
            let diff= &args[3];
            let out = &args[4];
            if let Err(e) = cmd_undiff(base, diff, out) {
                eprintln!("undiff failed: {}", e);
                std::process::exit(10);
            }
        }
        "screenshot" => {
            if args.len() < 8 { usage(&args[0]); std::process::exit(2); }
            let out = &args[2];
            let url= &args[3];
            let version = &args[4];
            let x1 = args[5].parse::<i64>().unwrap();
            let y1 = args[6].parse::<i64>().unwrap();
            let x2 = args[7].parse::<i64>().unwrap();
            let y2 = args[8].parse::<i64>().unwrap();
            if let Err(e) = cmd_screenshot(out, url, version, x1, y1, x2, y2) {
                eprintln!("screensot failed: {}", e);
                std::process::exit(11);
            }
        }
        "diffconvert" => {
            if args.len() < 3 { usage(&args[0]); std::process::exit(2); }
            let out = &args[2];
            let diff= &args[3];
            let workers = if args.len() > 4 {
                args[4].parse::<usize>().unwrap_or(10)
            } else {
                10
            };
            cmd_diffconvert(out, diff, workers).unwrap();
        }
        "zstbench" => {
            if args.len() < 2 { usage(&args[0]); std::process::exit(2); }
            let data = &args[2];
            cmd_zst_bench(data).unwrap();
        }
        "apgn" => {
            if args.len() < 3 { usage(&args[0]); std::process::exit(2); }
            let in_folder = &args[2];
            let out_file = &args[3];
            cmd_apgn(in_folder, out_file).unwrap();
        }
        _ => {
            usage(&args[0]);
            std::process::exit(1);
        }
    }
}
