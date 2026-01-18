use std::{collections::{HashMap, HashSet}, error::Error, f64::consts::PI, time::Duration, u16};

use png::{BitDepth, ColorType, Encoder};

use crate::palette;
use crate::{image::{self, PalettedImage}};

pub const WPLACE_TILES: i64 = 2<<11;
pub const WPLACE_PIXELS: i64 = WPLACE_TILES * 1000;

pub struct TileHistory {
    pub x: u16,
    pub y: u16,
    pub imgs: HashMap<u32, Vec<u8>>
}

impl TileHistory {
    pub fn from_bytes(x: u16, y: u16, data: &[u8]) -> anyhow::Result<TileHistory> {
        if data.len() < 8 {
            return Err(anyhow::anyhow!("data too short for TileHistory"));
        }
        let mut th = TileHistory {
            x,
            y,
            imgs: HashMap::new(),
        };
        let mut offset = 0;
        while offset < data.len() {
            if offset + 8 > data.len() {
                return Err(anyhow::anyhow!("data too short for TileHistory entry"));
            }
            let date_hours = u32::from_le_bytes([data[offset+0], data[offset+1], data[offset+2], data[offset+3]]) as usize;
            offset += 4;
            let block_size = u32::from_le_bytes([data[offset+0], data[offset+1], data[offset+2], data[offset+3]]) as usize;
            offset += 4;
            if offset + block_size > data.len() {
                return Err(anyhow::anyhow!("data too short for TileHistory image data"));
            }
            th.imgs.insert(date_hours as u32, data[offset..(offset+block_size)].to_vec());
            offset += block_size;
        }
        Ok(th)
    }
}

pub fn lng_to_world_coordinate(lng: f64, worldsize: i64) -> i64 {
    return (((180f64 + lng)/360f64) * (worldsize as f64)) as i64;
}

pub fn lat_to_world_coordinate(lat: f64, worldsize: i64) -> i64 {
    let y = f64::ln(f64::tan(45f64 + lat/2f64));
    return ((180f64 - y*(180f64 / PI))/360f64 * (worldsize as f64)) as i64;
}

pub fn copy_img(src: &PalettedImage, dst: &mut PalettedImage, tile_x_offset: i64, tile_y_offset: i64) {
    let offset_x = (tile_x_offset * 1000) as usize;
    let offset_y = (tile_y_offset * 1000) as usize;
    for y in 0..src.height {
        let src_row_start = y * src.width;
        let dst_row_start = (y + offset_y) * dst.width + offset_x;
        
        dst.indices[dst_row_start..dst_row_start + src.width]
            .copy_from_slice(&src.indices[src_row_start..src_row_start + src.width]);
    }
}

pub fn apply_diff_img(src: &PalettedImage, dst: &mut PalettedImage, tile_x_offset: i64, tile_y_offset: i64, background: u8) {
    let offset_x = (tile_x_offset * 1000) as usize;
    let offset_y = (tile_y_offset * 1000) as usize;
    for y in 0..src.height {
        let src_row_start = y * src.width;
        let dst_row_start = (y + offset_y) * dst.width + offset_x;
        
        for x in 0..src.width {
            let v = src.indices[src_row_start + x];
            if v != palette::DIFF_NO_CHANGE {
                if v == palette::TRANSPARENT {
                    dst.indices[dst_row_start + x] = background;
                } else {
                    dst.indices[dst_row_start + x] = v;
                }
            } else {
                dst.indices[dst_row_start + x] = palette::TRANSPARENT;
            }
        }
    }
}

pub fn init_img(x1: i64, y1: i64, x2: i64, y2: i64, background: u8) -> PalettedImage {
    assert!(x2 >= x1 && y2 >= y1);

    let height = ((y2+1)-y1)*1000;
    let width = ((x2+1)-x1)*1000;
    assert!(height < 20000 && width < 20000); // That's already 400MB of indices! Also few things will display a bigger image.
    PalettedImage { width: width as usize, height: height as usize, indices: vec![background; (width*height) as usize] }
}

#[cfg(feature = "native")]
pub fn native_screenshot(base_url: &str, version: &str, x1: i64, y1: i64, x2: i64, y2: i64) -> Result<PalettedImage, Box<dyn Error>> {
    use crate::palette::TRANSPARENT;

    let mut target = init_img(x1, y1, x2, y2, TRANSPARENT);

    let config = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(5))).http_status_as_error(false).build();
    let agent: ureq::Agent = config.into();

    for y in y1..(y2+1) {
        for x in x1..(x2+1) {
            let url = format!("{}/{}/11/{}/{}.png", base_url, version, x, y);
            let mut res = agent.get(url).call()?;
            let img = match res.status() {
                ureq::http::StatusCode::OK => {
                    let data = res.body_mut().read_to_vec()?;
                    image::compressed_bytes_to_paletted(&data)?
                },
                ureq::http::StatusCode::NOT_FOUND => {
                    PalettedImage { width: 1000, height: 1000, indices: vec![0u8; 1000*1000] }
                },
                s => return Err(format!("Unexpected status code: {}", s).into())
            };
            assert!(img.height == 1000 && img.width == 1000);
            copy_img(&img, &mut target, x-x1, y-y1);
        }
    }

    return Ok(target);
}

pub fn apng_from_history(history: HashMap<(u16, u16), TileHistory>, frame_delay_ms: u16) -> anyhow::Result<Vec<u8>> {
    assert!(history.len() >= 1, "need at least one tile history to create APNG");
    let mut date_set: HashSet<u32> = HashSet::new();
    let mut min_x: u16 = u16::MAX;
    let mut min_y: u16 = u16::MAX;
    let mut max_x: u16 = 0;
    let mut max_y: u16 = 0;


    for th in history.values() {
        for date in th.imgs.keys() {
            date_set.insert(*date);
        }
        if th.x < min_x {
            min_x = th.x;
        }
        if th.y < min_y {
            min_y = th.y;
        }
        if th.x > max_x {
            max_x = th.x;
        }
        if th.y > max_y {
            max_y = th.y;
        }
    }

    let sorted_dates: Vec<u32> = {
        let mut v: Vec<u32> = date_set.into_iter().collect();
        v.sort();
        v
    };

    let target_img = init_img(min_x as i64, min_y as i64, max_x as i64, max_y as i64, palette::WHITE);

    assert!(sorted_dates.len() >= 1, "need at least one frame for APNG");
    let mut out = Vec::new();
    let mut encoder = Encoder::new(&mut out, target_img.width as u32, target_img.height as u32);
    encoder.set_color(ColorType::Indexed);
    encoder.set_depth(BitDepth::Eight);
    encoder.set_compression(png::Compression::Balanced);

    // Build palette (RGB triples) and tRNS (alpha table)
    let (palette_bytes, trns) = palette::png_palette();
    encoder.set_palette(palette_bytes);
    encoder.set_trns(trns.as_slice());
    encoder.set_animated(sorted_dates.len() as u32, 0)?;
    encoder.set_blend_op(png::BlendOp::Over)?;
    encoder.set_frame_delay(frame_delay_ms, 1000)?;
    let mut writer = encoder.write_header()?;

    let mut first_frame = true;
    for date in sorted_dates {
        let mut frame_img = if first_frame { 
            first_frame = false;
            target_img.clone()
        } else { 
            init_img(min_x as i64, min_y as i64, max_x as i64, max_y as i64, palette::TRANSPARENT)
        };
        for y in min_y..(max_y+1) {
            for x in min_x..(max_x+1) {
                if let Some(th) = history.get(&(x, y)) {
                    if let Some(img_data) = th.imgs.get(&date) {
                        let img = image::compressed_bytes_to_paletted(img_data).unwrap();
                        apply_diff_img(&img, &mut frame_img, (x - min_x) as i64, (y - min_y) as i64, palette::WHITE);
                    }
                }
            }
        }

        writer.write_image_data(&frame_img.indices)?;
    }

    writer.finish()?;
    Ok(out)
}
