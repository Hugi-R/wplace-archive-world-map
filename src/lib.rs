mod image;
mod palette;
mod screenshot;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;
#[cfg(feature = "wasm")]
use wasm_bindgen_futures::JsFuture;
#[cfg(feature = "wasm")]
use web_sys::{Request, RequestInit, RequestMode, Response};

use crate::image::PalettedImage;

#[cfg(feature = "wasm")]
#[wasm_bindgen]
extern "C" {
    // Use `js_namespace` here to bind `console.log(..)` instead of just
    // `log(..)`
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

macro_rules! console_log {
    // Note that this is using the `log` function imported above during
    // `bare_bones`
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

/// Convert compressed paletted bytes to a PNG blob.
/// 
/// Takes zstd-compressed paletted image data (format: [width u32][height u32][indices...])
/// and returns the PNG data as a byte vector.
/// 
/// # Arguments
/// * `compressed` - The compressed paletted image data as a JavaScript Uint8Array (or Vec<u8>)
/// 
/// # Returns
/// A JavaScript Uint8Array containing the PNG file data
/// 
/// # Errors
/// Returns a JsValue error if decompression or PNG encoding fails
#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn compressed_bytes_to_png_blob(compressed: &[u8]) -> Result<Vec<u8>, wasm_bindgen::JsValue> {
    let paletted = image::compressed_bytes_to_paletted(compressed)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress: {}", e)))?;

    let mut png_bytes = Vec::new();
    {
        image::paletted_to_png(&paletted, &mut png_bytes)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to encode PNG: {}", e)))?;
    }

    Ok(png_bytes)
}

fn compressed_apply_diff(base_compressed: &[u8], diff_compressed: &[u8]) -> Result<PalettedImage, wasm_bindgen::JsValue> {
    console_log!("base={} diff={}", base_compressed.len(), diff_compressed.len());
    if (base_compressed.len() == 0) && (diff_compressed.len() == 0) {
        return Ok(image::PalettedImage{ height: 1000, width: 1000, indices: vec![0u8; 1000*1000] })
    } else if base_compressed.len() == 0 {
        let diff_paletted = image::compressed_bytes_to_paletted(diff_compressed)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress: {}", e)))?;
        return Ok(diff_paletted)
    } else if diff_compressed.len() == 0 {
        let base_paletted = image::compressed_bytes_to_paletted(base_compressed)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress: {}", e)))?;
        return Ok(base_paletted);
    } else {
        let base_paletted = image::compressed_bytes_to_paletted(base_compressed)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress: {}", e)))?;

        let diff_paletted = image::compressed_bytes_to_paletted(diff_compressed)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress: {}", e)))?;

        let paletted = image::apply_diff_paletted(&base_paletted, &diff_paletted);
        return Ok(paletted)
    }
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn diff_compressed_bytes_to_png_blob(base_compressed: &[u8], diff_compressed: &[u8]) -> Result<Vec<u8>, wasm_bindgen::JsValue> {

    let paletted = compressed_apply_diff(base_compressed, diff_compressed)?;

    let mut png_bytes = Vec::new();
    {
        image::paletted_to_png(&paletted, &mut png_bytes)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to encode PNG: {}", e)))?;
    }

    Ok(png_bytes)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn compressed_4to1(
    compressed1: &[u8],
    compressed2: &[u8],
    compressed3: &[u8],
    compressed4: &[u8],
) -> Result<Vec<u8>, wasm_bindgen::JsValue> {
    let p1 = if compressed1.len() == 0 {
        image::PalettedImage{ height: 1000, width: 1000, indices: vec![0u8; 1000*1000] }
    } else {
        image::compressed_bytes_to_paletted(compressed1).map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 1: {}", e)))?
    };
    let p2 = if compressed2.len() == 0 {
        image::PalettedImage{ height: 1000, width: 1000, indices: vec![0u8; 1000*1000] }
    } else {
        image::compressed_bytes_to_paletted(compressed2).map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 2: {}", e)))?
    };
    let p3 = if compressed3.len() == 0 {
        image::PalettedImage{ height: 1000, width: 1000, indices: vec![0u8; 1000*1000] }
    } else {
        image::compressed_bytes_to_paletted(compressed3).map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 3: {}", e)))?
    };
    let p4 = if compressed4.len() == 0 {
        image::PalettedImage{ height: 1000, width: 1000, indices: vec![0u8; 1000*1000] }
    } else {
        image::compressed_bytes_to_paletted(compressed4).map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 4: {}", e)))?
    };

    let mut weights = [100u32; 256];
    weights[0] = 0; // don't care about transparent pixels
    weights[1] = 50; // reduce importance of black pixels
    let res = image::downscale_4to1(&p1, &p2, &p3, &p4, &weights);

    let mut png_bytes = Vec::new();
    {
        image::paletted_to_png(&res, &mut png_bytes)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to encode PNG: {}", e)))?;
    }

    Ok(png_bytes)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn diff_compressed_4to1(
    base_compressed1: &[u8],
    base_compressed2: &[u8],
    base_compressed3: &[u8],
    base_compressed4: &[u8],
    diff_compressed1: &[u8],
    diff_compressed2: &[u8],
    diff_compressed3: &[u8],
    diff_compressed4: &[u8],
) -> Result<Vec<u8>, wasm_bindgen::JsValue> {
    let p1 = compressed_apply_diff(base_compressed1, diff_compressed1)?;
    let p2 = compressed_apply_diff(base_compressed2, diff_compressed2)?;
    let p3 = compressed_apply_diff(base_compressed3, diff_compressed3)?;
    let p4 = compressed_apply_diff(base_compressed4, diff_compressed4)?;


    let mut weights = [100u32; 256];
    weights[0] = 0; // don't care about transparent pixels
    weights[1] = 50; // reduce importance of black pixels
    let res = image::downscale_4to1(&p1, &p2, &p3, &p4, &weights);

    let mut png_bytes = Vec::new();
    {
        image::paletted_to_png(&res, &mut png_bytes)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to encode PNG: {}", e)))?;
    }

    Ok(png_bytes)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub async fn wasm_screenshot(base_url: &str, version: &str, x1: i64, y1: i64, x2: i64, y2: i64) -> Result<Vec<u8>, JsValue> {
    let mut target = screenshot::init_img(x1, y1, x2, y2, palette::TRANSPARENT);

    let opts = RequestInit::new();
    opts.set_method("GET");
    opts.set_mode(RequestMode::Cors);

    let window = web_sys::window().unwrap();

    for y in y1..(y2+1) {
        for x in x1..(x2+1) {
            let url = format!("{}/{}/11/{}/{}.png", base_url, version, x, y);
            let request = Request::new_with_str_and_init(&url, &opts)?;
            let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
            assert!(resp_value.is_instance_of::<Response>());
            let resp: Response = resp_value.dyn_into().unwrap();
            let img = match resp.status() {
                200 => {
                    use js_sys::Uint8Array;
                    let jsvalue = JsFuture::from(resp.array_buffer()?).await?;
                    let data = Uint8Array::new(&jsvalue).to_vec();
                    image::compressed_bytes_to_paletted(&data)
                        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decode compressed tile: {}", e)))?
                },
                404 => {
                    PalettedImage { width: 1000, height: 1000, indices: vec![0u8; 1000*1000] }
                },
                s => return Err(format!("Unexpected status code: {}", s).into())
            };
            assert!(img.height == 1000 && img.width == 1000);
            screenshot::copy_img(&img, &mut target, x-x1, y-y1);
        }
    }


    let mut png: Vec<u8> = Vec::new();
    {
        image::paletted_to_png(&target, &mut png)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to encode PNG: {}", e)))?;
    }

    Ok(png)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub async fn wasm_video(base_url: &str, x1: i64, y1: i64, x2: i64, y2: i64) -> Result<Vec<u8>, JsValue> {
    use std::collections::HashMap;
    
    let opts = RequestInit::new();
    opts.set_method("GET");
    opts.set_mode(RequestMode::Cors);

    let window = web_sys::window().unwrap();

    let mut history:HashMap<(u16, u16), screenshot::TileHistory> = HashMap::new();
    for y in y1..(y2+1) {
        for x in x1..(x2+1) {
            let url = format!("{}/all/11/{}/{}.zst", base_url, x, y);
            let request = Request::new_with_str_and_init(&url, &opts)?;
            let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
            assert!(resp_value.is_instance_of::<Response>());
            let resp: Response = resp_value.dyn_into().unwrap();
            match resp.status() {
                200 => {
                    use js_sys::Uint8Array;
                    let jsvalue = JsFuture::from(resp.array_buffer()?).await?;
                    let data = Uint8Array::new(&jsvalue).to_vec();
                    if data.len() > 0 {
                        screenshot::TileHistory::from_bytes(x as u16, y as u16, &data)
                            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decode tile history: {}", e)))
                            .map(|th| { history.insert((x as u16, y as u16), th); })?;
                    }
                },
                404 => {
                    // empty tile, do nothing
                },
                s => return Err(format!("Unexpected status code: {}", s).into())
            }
        }
    }

    let png = screenshot::apng_from_history(history, 200)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to create APNG: {}", e)))?;

    Ok(png)
}

fn images_from_history(data: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let mut builder = tar::Builder::new(Vec::new());

    let mut last_img: Option<PalettedImage> = None; 
    let mut offset = 0;
    while offset < data.len() {
        let date_hours = u32::from_le_bytes([data[offset+0], data[offset+1], data[offset+2], data[offset+3]]) as usize;
        offset += 4;
        let block_size = u32::from_le_bytes([data[offset+0], data[offset+1], data[offset+2], data[offset+3]]) as usize;
        offset += 4;
        console_log!("date {} size {}", date_hours, block_size);
        let diff_paletted = image::compressed_bytes_to_paletted(&data[offset..(offset+block_size)])?;
        offset += block_size;

        let paletted = if let Some(last) = &last_img {
            let applied = image::apply_diff_paletted(last, &diff_paletted);
            console_log!("applied diff");
            last_img = Some(applied);
            last_img.as_ref().unwrap()
        } else {
            last_img = Some(diff_paletted);
            last_img.as_ref().unwrap()
        };
        let png = paletted.to_png()?;

        let mut header = tar::Header::new_gnu();
        let filename = format!("{}.png", date_hours);
        header.set_path(filename)?;
        header.set_size(png.len() as u64);
        header.set_mode(0o644);
        let dt =chrono::DateTime::from_timestamp((date_hours as i64)*3600, 0).unwrap_or_default();
        header.set_mtime(dt.timestamp() as u64);
        header.set_cksum();
        builder.append(&header, &png[..])?;

    }

    let data = builder.into_inner()?;
    Ok(data)
}

