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
            let url = format!("{}/{}/11/{}/{}.zst", base_url, version, x, y);
            let request = Request::new_with_str_and_init(&url, &opts)?;
            let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
            assert!(resp_value.is_instance_of::<Response>());
            let resp: Response = resp_value.dyn_into().unwrap();
            let img = match resp.status() {
                200 => {
                    use js_sys::Uint8Array;
                    let jsvalue = JsFuture::from(resp.array_buffer()?).await?;
                    let data = Uint8Array::new(&jsvalue).to_vec();
                    match image_from_history(version, &data) {
                        Ok(img) => img,
                        Err(e) => {
                            if e == wasm_bindgen::JsValue::from_str("No images for requested version") || e == wasm_bindgen::JsValue::from_str("Empty data") {
                                PalettedImage { width: 1000, height: 1000, indices: vec![0u8; 1000*1000] }
                            } else {
                                return Err(e);
                            }
                        }
                    }
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
        image::paletted_to_png(&target, &mut png, true)
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

fn image_from_history(version: &str, data: &[u8]) -> Result<PalettedImage, wasm_bindgen::JsValue> {
    if data.len() == 0 {
        return Err(wasm_bindgen::JsValue::from_str("Empty data"));
    }
    let version_float = version.parse::<f32>()
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to parse version: {}", e)))?;
    let version_uint = (version_float * 1000.0) as u32;

    let tiles = screenshot::TileHistory::from_bytes(0, 0, &data)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decode tile history: {}", e)))?;

    // hasmap are not ordered, so we need to sort the keys
    let mut keys = tiles.imgs.keys().cloned().collect::<Vec<u32>>();
    keys.sort();
    // Keep keys that are <= version_uint
    keys = keys.into_iter().filter(|k| *k <= version_uint).collect::<Vec<u32>>();
    if keys.len() == 0 {
        return Err(wasm_bindgen::JsValue::from_str("No images for requested version"));
    }

    // Load base image
    let base_data = tiles.imgs.get(&keys[0]).unwrap();
    let mut base_paletted = image::compressed_bytes_to_paletted(&base_data)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress base image: {}", e)))?;

    // Apply diffs
    for key in keys.iter().skip(1) {
        let version_data = tiles.imgs.get(key).unwrap();
        let version_paletted = image::compressed_bytes_to_paletted(&version_data)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress version image: {}", e)))?;

        base_paletted = image::apply_diff_paletted(&base_paletted, &version_paletted);
    }
    Ok(base_paletted)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn get_image(version: &str, data: &[u8]) -> Result<Vec<u8>, wasm_bindgen::JsValue> {
    let base_paletted = image_from_history(version, data)?;
    base_paletted.to_png().map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to encode png: {}", e)))
}