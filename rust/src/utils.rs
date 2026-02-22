use std::{collections::{HashMap, HashSet}, f64::consts::PI, u16};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use png::{BitDepth, ColorType, Encoder};

use crate::{image::{self, CompressedImage, PalettedImage}, palette};

pub const WPLACE_TILES: i64 = 2<<11;
pub const WPLACE_PIXELS: i64 = WPLACE_TILES * 1000;

pub const ERR_TILE_HISTORY_NO_IMAGES: &str = "TileHistory has no images";
pub const ERR_NO_IMAGES_FOR_VERSION: &str = "No images for requested version";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DateHours(pub u32);

impl DateHours {
    /// Epoch: 2025-01-01 00:00:00 UTC
    const EPOCH: &'static str = "2025-01-01T00:00:00Z";

    pub fn min() -> Self {
        DateHours(0)
    }

    pub fn max() -> Self {
        DateHours(u32::MAX)
    }

    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        let epoch = DateTime::parse_from_rfc3339(Self::EPOCH)
            .unwrap()
            .with_timezone(&Utc);
        let duration = dt.signed_duration_since(epoch);
        let hours = duration.num_hours() as u32;
        DateHours(hours)
    }

    pub fn to_datetime(&self) -> DateTime<Utc> {
        let epoch = DateTime::parse_from_rfc3339(Self::EPOCH)
            .unwrap()
            .with_timezone(&Utc);
        epoch + ChronoDuration::hours(self.0 as i64)
    }

    pub fn week(&self) -> u32 {
        self.0 / (24 * 7)
    }

    pub fn day(&self) -> u32 {
        self.0 / 24
    }
}

/// Represents the history of a single tile, containing multiple versions of the tile image at different timestamps.
/// Each version is stored as a compressed diff image, keyed by the DateHours timestamp of when that version was created.
/// By convention, if the first key is 0, then that version is a full image. Otherwise, all versions are diffs that need to be applied on top of an empty tile.
pub struct TileHistory {
    pub x: u16,
    pub y: u16,
    pub imgs: HashMap<DateHours, CompressedImage>
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
            th.imgs.insert(DateHours(date_hours as u32), CompressedImage(data[offset..(offset+block_size)].to_vec()));
            offset += block_size;
        }
        Ok(th)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let sorted_date = {
            let mut v: Vec<DateHours> = self.imgs.keys().cloned().collect();
            v.sort();
            v
        };
        for date_hours in sorted_date {
            let img = self.imgs.get(&date_hours).unwrap();
            out.extend_from_slice(&date_hours.0.to_le_bytes());
            let img_data = &img.0;
            out.extend_from_slice(&(img_data.len() as u32).to_le_bytes());
            out.extend_from_slice(img_data);
        }
        out
    }

    /// Get the tile image for a specific timestamp by applying all diffs up to that timestamp on top of an empty tile.
    pub fn image(&self, until: DateHours) -> anyhow::Result<PalettedImage> {
        if self.imgs.is_empty() {
            return Err(anyhow::anyhow!(ERR_TILE_HISTORY_NO_IMAGES));
        }

        // hasmap are not ordered, so we need to sort the keys
        let mut keys = self.imgs.keys().cloned().collect::<Vec<DateHours>>();
        keys.sort();
        // Keep keys that are <= until
        keys = keys.into_iter().filter(|k| *k <= until).collect::<Vec<DateHours>>();
        if keys.len() == 0 {
            return Err(anyhow::anyhow!(ERR_NO_IMAGES_FOR_VERSION));
        }

        // Load base image
        let base_data = self.imgs.get(&keys[0]).unwrap();
        let mut base_paletted = base_data.to_paletted()?;

        // Apply diffs
        for key in keys.iter().skip(1) {
            let version_data = self.imgs.get(key).unwrap();
            let version_paletted = version_data.to_paletted()?;

            base_paletted = image::apply_diff_paletted(&base_paletted, &version_paletted);
        }
        Ok(base_paletted)
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

pub fn apng_from_history(history: HashMap<(u16, u16), TileHistory>, frame_delay_ms: u16) -> anyhow::Result<Vec<u8>> {
    assert!(history.len() >= 1, "need at least one tile history to create APNG");
    let mut date_set: HashSet<DateHours> = HashSet::new();
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

    let sorted_dates: Vec<DateHours> = {
        let mut v: Vec<DateHours> = date_set.into_iter().collect();
        v.sort_by_key(|d| d.0);
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
                        let img = img_data.to_paletted().unwrap();
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
