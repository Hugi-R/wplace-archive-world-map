use std::fs::File;
use std::io::{Read, Write, Cursor, BufReader, BufRead, Seek};
use std::path::Path;
use std::error::Error;

use png::{Decoder, Encoder, ColorType, BitDepth};

use crate::palette::{index_from_rgba, PALETTE};

/// In-memory representation of a paletted image
pub struct PalettedImage {
    pub width: usize,
    pub height: usize,
    pub indices: Vec<u8>,
}

/// Convert a PNG stream (from BufReader) to a paletted image representation.
pub fn png_to_paletted<R: BufRead + Seek>(reader: R) -> Result<PalettedImage, Box<dyn Error>> {
    let decoder = Decoder::new(reader);
    let mut png_reader = decoder.read_info()?;
    // Require reader-provided buffer size; otherwise return an error.
    let buf_size = png_reader.output_buffer_size()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "PNG decoder did not provide an output buffer size"))?;
    let mut buf = vec![0u8; buf_size];
    png_reader.next_frame(&mut buf)?;
    let info = png_reader.info();
    let rgba = expand_to_rgba8(&info.color_type, &info.bit_depth, &buf, info)?;

    let width = info.width as usize;
    let height = info.height as usize;
    if rgba.len() != width * height * 4 {
        return Err(format!("unexpected rgba length {} for {}x{}", rgba.len(), width, height).into());
    }

    // Map RGBA -> palette index
    let mut indices = Vec::with_capacity(width * height);
    for px in rgba.chunks_exact(4) {
        let mut rgba4 = [0u8;4];
        rgba4.copy_from_slice(px);
        let idx = index_from_rgba(rgba4);
        indices.push(idx);
    }

    Ok(PalettedImage {
        width: width,
        height: height,
        indices,
    })
}

/// Read a PNG from `input_path` and convert it to a paletted image representation.
pub fn png_file_to_paletted(input_path: &Path) -> Result<PalettedImage, Box<dyn Error>> {
    let f = File::open(input_path)?;
    let br = BufReader::new(f);
    png_to_paletted(br)
}

/// Convert a paletted image to zstd-compressed bytes.
///
/// Uncompressed format:
/// [u32 little-endian width][u32 little-endian height][width*height bytes of u8 indices]
pub fn paletted_to_compressed_bytes(paletted: &PalettedImage) -> Result<Vec<u8>, Box<dyn Error>> {
    // Serialize metadata + indices
    let mut out = Vec::with_capacity(8 + paletted.indices.len());
    out.extend(&((paletted.width as u32).to_le_bytes()));
    out.extend(&((paletted.height as u32).to_le_bytes()));
    out.extend(&paletted.indices);

    // Compress with zstd
    let mut enc = zstd::Encoder::new(Vec::new(), 10)?; // level 10 is good compression and speed. Level 12 is a lot slower. Even level 6 is worse.
    enc.write_all(&out)?;
    let compressed = enc.finish()?;

    Ok(compressed)
}

/// Read the zstd-compressed paletted byte array (the format written by
/// `paletted_to_compressed_bytes`) and convert it to a paletted image representation.
pub fn compressed_bytes_to_paletted(compressed: &[u8]) -> Result<PalettedImage, Box<dyn Error>> {
    let mut dec = zstd::Decoder::new(Cursor::new(compressed))?;
    let mut decompressed = Vec::new();
    dec.read_to_end(&mut decompressed)?;

    if decompressed.len() < 8 {
        return Err("decompressed data too short".into());
    }
    let width = u32::from_le_bytes([decompressed[0], decompressed[1], decompressed[2], decompressed[3]]);
    let height = u32::from_le_bytes([decompressed[4], decompressed[5], decompressed[6], decompressed[7]]);
    let expected = (width as usize) * (height as usize);
    if decompressed.len() != 8 + expected {
        return Err(format!("decompressed length mismatch: expected {} bytes of indices, got {}", expected, decompressed.len() - 8).into());
    }
    let indices = decompressed[8..].to_vec();

    Ok(PalettedImage {
        width: width as usize,
        height: height as usize,
        indices,
    })
}

/// Convert a paletted image back to a PNG.
pub fn paletted_to_png<W: Write>(paletted: &PalettedImage, out: W) -> Result<(), Box<dyn Error>> {
    {
        let mut encoder = Encoder::new(out, paletted.width as u32, paletted.height as u32);
        encoder.set_color(ColorType::Indexed);
        encoder.set_depth(BitDepth::Eight);
        encoder.set_compression(png::Compression::Fastest); // Fast or Fastest are good choices

        // Build palette (RGB triples) and tRNS (alpha table)
        let mut palette_bytes = Vec::with_capacity(256 * 3);
        let mut trns = Vec::with_capacity(256);
        for rgba in PALETTE.iter() {
            palette_bytes.push(rgba[0]);
            palette_bytes.push(rgba[1]);
            palette_bytes.push(rgba[2]);
            trns.push(rgba[3]);
        }
        encoder.set_palette(palette_bytes);
        encoder.set_trns(trns.as_slice());

        let mut writer = encoder.write_header()?;
        writer.write_image_data(&paletted.indices)?;
    }
    Ok(())
}

/// Convert a paletted image back to a PNG file.
pub fn paletted_to_png_file(paletted: &PalettedImage, output_path: &Path) -> Result<(), Box<dyn Error>> {
    let mut of = File::create(output_path)?;
    paletted_to_png(paletted,  of)?;
    Ok(())
}

/// Read a PNG from `input_path`, convert it to palette indices using `palette.rs`,
/// and write a zstd-compressed byte array to `output_path`.
///
/// Stored format (before compression):
/// [u32 little-endian width][u32 little-endian height][width*height bytes of u8 indices]
pub fn png_file_to_compressed_paletted(input_path: &Path, output_path: &Path) -> Result<(), Box<dyn Error>> {
    let paletted = png_file_to_paletted(input_path)?;
    let compressed = paletted_to_compressed_bytes(&paletted)?;

    let mut of = File::create(output_path)?;
    of.write_all(&compressed)?;
    Ok(())
}

/// Read the zstd-compressed paletted byte array from `input_path` and write a paletted PNG to `output_path`.
pub fn compressed_paletted_to_png(input_path: &Path, output_path: &Path) -> Result<(), Box<dyn Error>> {
    let mut f = File::open(input_path)?;
    let mut compressed = Vec::new();
    f.read_to_end(&mut compressed)?;

    let paletted = compressed_bytes_to_paletted(&compressed)?;
    paletted_to_png_file(&paletted, output_path)
}

pub fn downscale_4to1(p1: &PalettedImage, p2: &PalettedImage, p3: &PalettedImage, p4: &PalettedImage, weights: &[u32; 256]) -> PalettedImage {
    assert!(p1.width == p2.width && p1.height == p2.height);
    assert!(p1.width == p3.width && p1.height == p3.height);
    assert!(p1.width == p4.width && p1.height == p4.height);

    // Downscale each part by 2x using weighted mode
    let r1 = downscale_mode_weighted_2x2(&p1.indices, p1.width, p1.height, weights);
    let r2 = downscale_mode_weighted_2x2(&p2.indices, p2.width, p2.height, weights);
    let r3 = downscale_mode_weighted_2x2(&p3.indices, p3.width, p3.height, weights);
    let r4 = downscale_mode_weighted_2x2(&p4.indices, p4.width, p4.height, weights);

    // Merge the four results as a 2x2 grid
    let out_w = p1.width;
    let out_h = p1.height;
    let mut res = PalettedImage {
        width: out_w,
        height: out_h,
        indices: vec![0u8; out_w * out_h],
    };
    for oy in 0..(out_h / 2) {
        let row1 = oy * out_w;
        let row2 = (oy + out_h / 2) * out_w;
        let r1_base = oy * (out_w / 2);
        let r2_base = oy * (out_w / 2);
        for ox in 0..(out_w / 2) {
            res.indices[row1 + ox] = r1[r1_base + ox];
            res.indices[row1 + ox + (out_w / 2)] = r2[r2_base + ox];
            res.indices[row2 + ox] = r3[r1_base + ox];
            res.indices[row2 + ox + (out_w / 2)] = r4[r2_base + ox];
        }
    }

    res
}

pub fn downscale_mode_weighted(
    src_idx: &[u8],
    src_w: usize,
    src_h: usize,
    weights: &[u32; 256],
    block_size: usize,
) -> Vec<u8> {
    assert!(block_size >= 2);
    assert!(block_size <= 8);
    assert!(src_h % block_size == 0);
    assert!(src_w % block_size == 0);
    let out_w = src_w / block_size;
    let out_h = src_h / block_size;

    let mut out = vec![0u8; out_w * out_h];

    // Reused scratch (small, cache-friendly)
    let mut scores = [0u32; 256];
    let mut stamp = [0u32; 256];
    let mut cur_stamp: u32 = 1;

    for oy in 0..out_h {
        let sy0 = oy * block_size;
        for ox in 0..out_w {
            let sx0 = ox * block_size;

            let mut touched = [0u8; 64]; // worst case all different
            let mut touched_len = 0usize;

            // Vote over the block
            for dy in 0..block_size {
                let row = (sy0 + dy) * src_w;
                let base = row + sx0;
                for dx in 0..block_size {
                    let idx = src_idx[base + dx] as usize;

                    if stamp[idx] != cur_stamp {
                        stamp[idx] = cur_stamp;
                        scores[idx] = weights[idx];
                        touched[touched_len] = idx as u8;
                        touched_len += 1;
                    } else {
                        scores[idx] += weights[idx];
                    }
                }
            }

            // Argmax over touched indices only
            let mut best = touched[0] as usize;
            let mut best_score = scores[best];
            for i in 1..touched_len {
                let c = touched[i] as usize;
                let s = scores[c];
                // tie-break: keep existing, or pick lower index, or center pixel, your choice
                if s > best_score {
                    best = c;
                    best_score = s;
                }
            }

            out[oy * out_w + ox] = best as u8;
            cur_stamp = cur_stamp.wrapping_add(1);
            if cur_stamp == 0 {
                // extremely unlikely here, but keep it safe
                stamp.fill(0);
                cur_stamp = 1;
            }
        }
    }

    out
}

pub fn downscale_mode_weighted_2x2(
    src_idx: &[u8],
    src_w: usize,
    src_h: usize,
    weights: &[u32; 256],
) -> Vec<u8> {
    assert!(src_h % 2 == 0);
    assert!(src_w % 2 == 0);
    let out_w = src_w / 2;
    let out_h = src_h / 2;

    let mut out = vec![0u8; out_w * out_h];

    for oy in 0..out_h {
        let sy0 = oy * 2;
        let row0_base = sy0 * src_w;
        let row1_base = (sy0 + 1) * src_w;
        for ox in 0..out_w {
            let sx0 = ox * 2;

            let mut scores = [0u32; 4];
            let colors = [
                src_idx[row0_base + sx0],
                src_idx[row0_base + sx0 + 1],
                src_idx[row1_base + sx0],
                src_idx[row1_base + sx0 + 1],
            ];

            scores[0] = weights[colors[0] as usize];

            if colors[1] == colors[0] {
                scores[0] += weights[colors[1] as usize];
            } else {
                scores[1] = weights[colors[1] as usize];
            }

            if colors[2] == colors[0] {
                scores[0] += weights[colors[2] as usize];
            } else if colors[2] == colors[1] {
                scores[1] += weights[colors[2] as usize];
            } else {
                scores[2] = weights[colors[2] as usize];
            }

             if colors[3] == colors[0] {
                scores[0] += weights[colors[3] as usize];
            } else if colors[3] == colors[1] {
                scores[1] += weights[colors[3] as usize];
            } else if colors[3] == colors[2] {
                scores[2] += weights[colors[3] as usize];
            } else {
                scores[3] = weights[colors[3] as usize];
            }

            out[oy * out_w + ox] = colors[argmax(&scores)];
        }
    }

    out
}

fn argmax(a: &[u32]) -> usize {
    let mut best_idx = 0usize;
    let mut best_val = a[0];
    for (i, &v) in a.iter().enumerate().skip(1) {
        if v > best_val {
            best_val = v;
            best_idx = i;
        }
    }
    best_idx
}

// -- helpers ---------------------------------------------------------------

fn expand_to_rgba8(color: &ColorType, bit_depth: &BitDepth, buf: &[u8], info: &png::Info) -> Result<Vec<u8>, Box<dyn Error>> {
    match color {
        ColorType::Rgba => {
            match bit_depth {
                BitDepth::Eight => Ok(buf.to_vec()),
                BitDepth::Sixteen => {
                    // downsample: RGBA 16-bit -> take the high byte of each 16-bit sample
                    // 8 bytes per pixel (R_hi,R_lo,G_hi,G_lo,B_hi,B_lo,A_hi,A_lo)
                    let mut out = Vec::with_capacity(buf.len() / 2);
                    for px in buf.chunks_exact(8) {
                        out.push(px[0]); out.push(px[2]); out.push(px[4]); out.push(px[6]);
                    }
                    Ok(out)
                }
                _ => Err(format!("unsupported bit depth for Rgba: {:?}", bit_depth).into())
            }
        }
        ColorType::Rgb => {
            let mut out = Vec::with_capacity((info.width as usize * info.height as usize) * 4);
            match bit_depth {
                BitDepth::Eight => {
                    for px in buf.chunks_exact(3) {
                        out.push(px[0]); out.push(px[1]); out.push(px[2]); out.push(255);
                    }
                    Ok(out)
                }
                BitDepth::Sixteen => {
                    for px in buf.chunks_exact(6) {
                        out.push(px[0]); out.push(px[2]); out.push(px[4]); out.push(255);
                    }
                    Ok(out)
                }
                _ => Err(format!("unsupported bit depth for RGB: {:?}", bit_depth).into())
            }
        }
        ColorType::Grayscale => {
            let mut out = Vec::with_capacity((info.width as usize * info.height as usize) * 4);
            match bit_depth {
                BitDepth::Eight => {
                    for g in buf.iter() {
                        out.push(*g); out.push(*g); out.push(*g); out.push(255);
                    }
                    Ok(out)
                }
                BitDepth::Sixteen => {
                    for px in buf.chunks_exact(2) {
                        let g = px[0];
                        out.push(g); out.push(g); out.push(g); out.push(255);
                    }
                    Ok(out)
                }
                _ => Err(format!("unsupported bit depth for Grayscale: {:?}", bit_depth).into())
            }
        }
        ColorType::GrayscaleAlpha => {
            let mut out = Vec::with_capacity((info.width as usize * info.height as usize) * 4);
            match bit_depth {
                BitDepth::Eight => {
                    for px in buf.chunks_exact(2) {
                        let g = px[0]; let a = px[1];
                        out.push(g); out.push(g); out.push(g); out.push(a);
                    }
                    Ok(out)
                }
                BitDepth::Sixteen => {
                    for px in buf.chunks_exact(4) {
                        let g = px[0]; let a = px[2];
                        out.push(g); out.push(g); out.push(g); out.push(a);
                    }
                    Ok(out)
                }
                _ => Err(format!("unsupported bit depth for GrayscaleAlpha: {:?}", bit_depth).into())
            }
        }
        ColorType::Indexed => {
            // buf contains indices, possibly packed if bit depth < 8.
            let pixel_count = (info.width as usize) * (info.height as usize);
            let indices = match bit_depth {
                BitDepth::Eight => buf.to_vec(),
                BitDepth::Sixteen => return Err("unexpected 16-bit for Indexed".into()),
                BitDepth::One | BitDepth::Two | BitDepth::Four => unpack_indices(buf, *bit_depth, pixel_count)?,
                _ => return Err(format!("unsupported bit depth for Indexed: {:?}", bit_depth).into()),
            };

            // palette present in info.palette as RGB triples
            let palette = info.palette.as_ref().ok_or("indexed PNG without palette")?;
            let trns = info.trns.as_ref();
            let mut out = Vec::with_capacity(pixel_count * 4);
            for &idx in indices.iter() {
                let i = idx as usize;
                let base = i * 3;
                if base + 2 >= palette.len() {
                    // default to magenta/debug
                    out.push(255); out.push(0); out.push(255);
                } else {
                    out.push(palette[base]);
                    out.push(palette[base + 1]);
                    out.push(palette[base + 2]);
                }
                let a = trns.and_then(|t| t.get(i)).cloned().unwrap_or(255);
                out.push(a);
            }
            Ok(out)
        }
        _ => Err(format!("unsupported color type: {:?}", color).into())
    }
}

fn unpack_indices(buf: &[u8], bit_depth: BitDepth, pixel_count: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut out = Vec::with_capacity(pixel_count);
    match bit_depth {
        BitDepth::One => {
            for &b in buf.iter() {
                for bit in 0..8 {
                    if out.len() >= pixel_count { break; }
                    let shift = 7 - bit;
                    let val = (b >> shift) & 0x01;
                    out.push(val);
                }
            }
        }
        BitDepth::Two => {
            for &b in buf.iter() {
                for shift in (0..8).step_by(2).rev() { // 6,4,2,0
                    if out.len() >= pixel_count { break; }
                    let val = (b >> shift) & 0x03;
                    out.push(val);
                }
            }
        }
        BitDepth::Four => {
            for &b in buf.iter() {
                for &shift in &[4usize, 0usize] {
                    if out.len() >= pixel_count { break; }
                    let val = (b >> shift) & 0x0F;
                    out.push(val);
                }
            }
        }
        _ => return Err("unsupported small bit depth".into()),
    }
    out.truncate(pixel_count);
    Ok(out)
}
