use std::{error::Error, f64::consts::PI, io, time::Duration};

use ureq::http::StatusCode;

use crate::image::{self, PalettedImage};

pub const WPLACE_TILES: i64 = 2<<11;
pub const WPLACE_PIXELS: i64 = WPLACE_TILES * 1000;

pub fn lng_to_world_coordinate(lng: f64, worldsize: i64) -> i64 {
    return (((180f64 + lng)/360f64) * (worldsize as f64)) as i64;
}

pub fn lat_to_world_coordinate(lat: f64, worldsize: i64) -> i64 {
    let y = f64::ln(f64::tan(45f64 + lat/2f64));
    return ((180f64 - y*(180f64 / PI))/360f64 * (worldsize as f64)) as i64;
}

fn copy_img(src: &PalettedImage, dst: &mut PalettedImage, tile_x_offset: i64, tile_y_offset: i64) {
    let offset_x = (tile_x_offset * 1000) as usize;
    let offset_y = (tile_y_offset * 1000) as usize;
    for y in 0..src.height {
        let src_row_start = y * src.width;
        let dst_row_start = (y + offset_y) * dst.width + offset_x;
        
        dst.indices[dst_row_start..dst_row_start + src.width]
            .copy_from_slice(&src.indices[src_row_start..src_row_start + src.width]);
    }
}

pub fn screenshot(base_url: &str, version: &str, x1: i64, y1: i64, x2: i64, y2: i64) -> Result<PalettedImage, Box<dyn Error>> {
    assert!(x2 >= x1 && y2 >= y1);

    let height = ((y2+1)-y1)*1000;
    let width = ((x2+1)-x1)*1000;
    assert!(height < 20000 && width < 20000); // That's already 400MB of indices! Also few things will display a bigger image.
    let mut target = PalettedImage { width: width as usize, height: height as usize, indices: vec![0u8; (width*height) as usize] };

    let config = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(5))).http_status_as_error(false).build();
    let agent: ureq::Agent = config.into();

    for y in y1..(y2+1) {
        for x in x1..(x2+1) {
            let url = format!("{}/{}/11/{}/{}.png", base_url, version, x, y);
            let mut res = agent.get(url).call()?;
            let img = match res.status() {
                StatusCode::OK => {
                    let data = res.body_mut().read_to_vec()?;
                    image::compressed_bytes_to_paletted(&data)?
                },
                StatusCode::NOT_FOUND => {
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