use once_cell::sync::Lazy;
use std::collections::HashMap;
use wplacetools;

pub const PALETTE_SIZE: usize = 256;
pub const TRANSPARENT: u8 = 0u8;
pub const BLACK: u8 = 1u8;
pub const WHITE: u8 = 5u8;
pub const DEBUG_COLOR: u8 = 255u8;
pub const DIFF_NO_CHANGE: u8 = 254u8;

/// Palette: index -> RGBA (r,g,b,a)
pub static PALETTE: Lazy<[[u8; 4]; PALETTE_SIZE]> = Lazy::new(|| {
    let mut a = [[255u8, 0, 255, 255]; PALETTE_SIZE];

    // reserve 0 for transparent
    a[TRANSPARENT as usize] = [0, 0, 0, 0]; // Transparent

    // default debug color at 255 (magenta)
    a[DEBUG_COLOR as usize] = [255, 0, 255, 255]; // Debug (unknown)

    // reserved 254 for diff color
    a[DIFF_NO_CHANGE as usize] = [255, 1, 255, 255]; // Diff (reserved)

    macro_rules! set {
        ($idx:expr, $r:expr, $g:expr, $b:expr) => {
            a[$idx as usize] = [$r as u8, $g as u8, $b as u8, 255];
        };
    }

    // Ordered ascending by index with color name comments
    set!(BLACK, 0, 0, 0); // Black
    set!(2, 60, 60, 60); // Dark Gray
    set!(3, 120, 120, 120); // Gray
    set!(4, 210, 210, 210); // Light Gray
    set!(WHITE, 255, 255, 255); // White
    set!(6, 96, 0, 24); // Deep Red
    set!(7, 237, 28, 36); // Red
    set!(8, 255, 127, 39); // Orange
    set!(9, 246, 170, 9); // Gold
    set!(10, 249, 221, 59); // Yellow
    set!(11, 255, 250, 188); // Light Yellow
    set!(12, 14, 185, 104); // Dark Green
    set!(13, 19, 230, 123); // Green
    set!(14, 135, 255, 94); // Light Green
    set!(15, 12, 129, 110); // Dark Teal
    set!(16, 16, 174, 166); // Teal
    set!(17, 19, 225, 190); // Light Teal
    set!(18, 40, 80, 158); // Dark Blue
    set!(19, 64, 147, 228); // Blue
    set!(20, 96, 247, 242); // Cyan
    set!(21, 107, 80, 246); // Indigo
    set!(22, 153, 177, 251); // Light Indigo
    set!(23, 120, 12, 153); // Dark Purple
    set!(24, 170, 56, 185); // Purple
    set!(25, 224, 159, 249); // Light Purple
    set!(26, 203, 0, 122); // Dark Pink
    set!(27, 236, 31, 128); // Pink
    set!(28, 243, 141, 169); // Light Pink
    set!(29, 104, 70, 52); // Dark Brown
    set!(30, 149, 104, 42); // Brown
    set!(31, 248, 178, 119); // Beige
    set!(32, 170, 170, 170); // Medium Gray
    set!(33, 165, 14, 30); // Dark Red
    set!(34, 250, 128, 114); // Light Red
    set!(35, 228, 92, 26); // Dark Orange
    set!(36, 214, 181, 148); // Light Tan
    set!(37, 156, 132, 49); // Dark Goldenrod
    set!(38, 197, 173, 49); // Goldenrod
    set!(39, 232, 212, 95); // Light Goldenrod
    set!(40, 74, 107, 58); // Dark Olive
    set!(41, 90, 148, 74); // Olive
    set!(42, 132, 197, 115); // Light Olive
    set!(43, 15, 121, 159); // Dark Cyan
    set!(44, 187, 250, 242); // Light Cyan
    set!(45, 125, 199, 255); // Light Blue
    set!(46, 77, 49, 184); // Dark Indigo
    set!(47, 74, 66, 132); // Dark Slate Blue
    set!(48, 122, 113, 196); // Slate Blue
    set!(49, 181, 174, 241); // Light Slate Blue
    set!(50, 219, 164, 99); // Light Brown
    set!(51, 209, 128, 81); // Dark Beige
    set!(52, 255, 197, 165); // Light Beige
    set!(53, 155, 82, 73); // Dark Peach
    set!(54, 209, 128, 120); // Peach
    set!(55, 250, 182, 164); // Light Peach
    set!(56, 123, 99, 82); // Dark Tan
    set!(57, 156, 132, 107); // Tan
    set!(58, 51, 57, 65); // Dark Slate
    set!(59, 109, 117, 141); // Slate
    set!(60, 179, 185, 209); // Light Slate
    set!(61, 109, 100, 63); // Dark Stone
    set!(62, 148, 140, 107); // Stone
    set!(63, 205, 197, 158); // Light Stone

    a
});

/// Inverse lookup: RGBA -> index (u8). Unknown RGBA -> 255 (debug).
pub static PALETTE_INV: Lazy<HashMap<[u8; 4], u8>> = Lazy::new(|| {
    let mut m = HashMap::with_capacity(PALETTE_SIZE);
    for (i, rgba) in PALETTE.iter().enumerate() {
        m.insert(*rgba, i as u8);
    }
    m
});

/// Get RGBA for palette index. index 0 is transparent.
pub fn rgba_from_index(i: u8) -> [u8; 4] {
    PALETTE[i as usize]
}

/// Get index for RGBA. Returns 255 if RGBA not found.
pub fn index_from_rgba(rgba: [u8; 4]) -> u8 {
    *PALETTE_INV.get(&rgba).unwrap_or(&255u8)
}

/// wplacetools' palette: index -> this palette: index
pub static WPLACETOOLS_PALETTE_CONVERSION: Lazy<[u8; PALETTE_SIZE]> = Lazy::new(|| {
    let mut a = [255u8; PALETTE_SIZE];

    // Special case for transparent, as wplacetools palette does not store alpha channel
    a[0] = TRANSPARENT;

    for i in 1..wplacetools::GLOBAL_PALETTE.len() {
        let [r, g, b] = wplacetools::GLOBAL_PALETTE[i];
        let this_index = index_from_rgba([r, g, b, 255]);
        a[i] = this_index;
    }

    a
});

pub fn png_palette() -> (Vec<u8>, Vec<u8>) {
    let mut palette_bytes = Vec::with_capacity(256 * 3);
    let mut trns = Vec::with_capacity(256);
    for rgba in PALETTE.iter() {
        palette_bytes.push(rgba[0]);
        palette_bytes.push(rgba[1]);
        palette_bytes.push(rgba[2]);
        trns.push(rgba[3]);
    }
    (palette_bytes, trns)
}