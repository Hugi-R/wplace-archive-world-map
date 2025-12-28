mod image;
mod palette;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

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

#[cfg(feature = "wasm")]
#[wasm_bindgen]
pub fn compressed_4to1(
    compressed1: &[u8],
    compressed2: &[u8],
    compressed3: &[u8],
    compressed4: &[u8],
) -> Result<Vec<u8>, wasm_bindgen::JsValue> {
    let p1 = image::compressed_bytes_to_paletted(compressed1)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 1: {}", e)))?;
    let p2 = image::compressed_bytes_to_paletted(compressed2)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 2: {}", e)))?;
    let p3 = image::compressed_bytes_to_paletted(compressed3)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 3: {}", e)))?;
    let p4 = image::compressed_bytes_to_paletted(compressed4)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress image 4: {}", e)))?;

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