mod image;
mod palette;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

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
    // Decompress the paletted image from the compressed bytes
    let paletted = image::compressed_bytes_to_paletted(compressed)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to decompress: {}", e)))?;

    // Convert paletted image to PNG bytes
    let mut png_bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut png_bytes, paletted.width as u32, paletted.height as u32);
        encoder.set_color(png::ColorType::Indexed);
        encoder.set_depth(png::BitDepth::Eight);

        // Build palette (RGB triples) and tRNS (alpha table)
        let mut palette_bytes = Vec::with_capacity(256 * 3);
        let mut trns = Vec::with_capacity(256);
        for rgba in palette::PALETTE.iter() {
            palette_bytes.push(rgba[0]);
            palette_bytes.push(rgba[1]);
            palette_bytes.push(rgba[2]);
            trns.push(rgba[3]);
        }
        encoder.set_palette(palette_bytes);
        encoder.set_trns(trns.as_slice());

        let mut writer = encoder.write_header()
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to write PNG header: {}", e)))?;
        writer.write_image_data(&paletted.indices)
            .map_err(|e| wasm_bindgen::JsValue::from_str(&format!("Failed to write PNG data: {}", e)))?;
    }

    Ok(png_bytes)
}
