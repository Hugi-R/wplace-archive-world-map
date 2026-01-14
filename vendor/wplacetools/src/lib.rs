pub mod diff;

// Taken From https://github.com/bczhc/wplace-tools
// 
// MIT License
// 
// Copyright (c) 2025 Zhai Can
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// src/lib.rs - 30d8f1b - 2026-01-03
pub const CHUNK_WIDTH: usize = 1000;
pub const CHUNK_LENGTH: usize = CHUNK_WIDTH * CHUNK_WIDTH;
pub const CHUNK_DIMENSION: (u32, u32) = (CHUNK_WIDTH as u32, CHUNK_WIDTH as u32);
pub const MUTATION_MASK: u8 = 0b0100_0000;
pub const PALETTE_INDEX_MASK: u8 = 0b0011_1111;

pub type ChunkNumber = (u16, u16);

pub const GLOBAL_PALETTE: [[u8; 3]; 64] = [
    // transparency
    [0, 0, 0],
    // black
    [0, 0, 0],
    [60, 60, 60],
    [120, 120, 120],
    [170, 170, 170],
    [210, 210, 210],
    [255, 255, 255],
    [96, 0, 24],
    [165, 14, 30],
    [237, 28, 36],
    [250, 128, 114],
    [228, 92, 26],
    [255, 127, 39],
    [246, 170, 9],
    [249, 221, 59],
    [255, 250, 188],
    [156, 132, 49],
    [197, 173, 49],
    [232, 212, 95],
    [74, 107, 58],
    [90, 148, 74],
    [132, 197, 115],
    [14, 185, 104],
    [19, 230, 123],
    [135, 255, 94],
    [12, 129, 110],
    [16, 174, 166],
    [19, 225, 190],
    [15, 121, 159],
    [96, 247, 242],
    [187, 250, 242],
    [40, 80, 158],
    [64, 147, 228],
    [125, 199, 255],
    [77, 49, 184],
    [107, 80, 246],
    [153, 177, 251],
    [74, 66, 132],
    [122, 113, 196],
    [181, 174, 241],
    [120, 12, 153],
    [170, 56, 185],
    [224, 159, 249],
    [203, 0, 122],
    [236, 31, 128],
    [243, 141, 169],
    [155, 82, 73],
    [209, 128, 120],
    [250, 182, 164],
    [104, 70, 52],
    [149, 104, 42],
    [219, 164, 99],
    [123, 99, 82],
    [156, 132, 107],
    [214, 181, 148],
    [209, 128, 81],
    [248, 178, 119],
    [255, 197, 165],
    [109, 100, 63],
    [148, 140, 107],
    [205, 197, 158],
    [51, 57, 65],
    [109, 117, 141],
    [179, 185, 209],
];