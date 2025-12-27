use std::error::Error;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::time::Instant;

mod image;
mod palette;
mod tilesdb;
mod ingest;
mod reader_targz;

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
        _ => {
            usage(&args[0]);
            std::process::exit(1);
        }
    }
}
