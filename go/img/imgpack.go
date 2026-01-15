package img

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
)

var colorToIndex = map[[3]uint8]int{
	{0, 0, 0}:       1,  // Black
	{60, 60, 60}:    2,  // Dark Gray
	{120, 120, 120}: 3,  // Gray
	{170, 170, 170}: 32, // Medium Gray
	{210, 210, 210}: 4,  // Light Gray
	{255, 255, 255}: 5,  // White
	{96, 0, 24}:     6,  // Deep Red
	{165, 14, 30}:   33, // Dark Red
	{237, 28, 36}:   7,  // Red
	{250, 128, 114}: 34, // Light Red
	{228, 92, 26}:   35, // Dark Orange
	{255, 127, 39}:  8,  // Orange
	{246, 170, 9}:   9,  // Gold
	{249, 221, 59}:  10, // Yellow
	{255, 250, 188}: 11, // Light Yellow
	{156, 132, 49}:  37, // Dark Goldenrod
	{197, 173, 49}:  38, // Goldenrod
	{232, 212, 95}:  39, // Light Goldenrod
	{74, 107, 58}:   40, // Dark Olive
	{90, 148, 74}:   41, // Olive
	{132, 197, 115}: 42, // Light Olive
	{14, 185, 104}:  12, // Dark Green
	{19, 230, 123}:  13, // Green
	{135, 255, 94}:  14, // Light Green
	{12, 129, 110}:  15, // Dark Teal
	{16, 174, 166}:  16, // Teal
	{19, 225, 190}:  17, // Light Teal
	{15, 121, 159}:  43, // Dark Cyan
	{96, 247, 242}:  20, // Cyan
	{187, 250, 242}: 44, // Light Cyan
	{40, 80, 158}:   18, // Dark Blue
	{64, 147, 228}:  19, // Blue
	{125, 199, 255}: 45, // Light Blue
	{77, 49, 184}:   46, // Dark Indigo
	{107, 80, 246}:  21, // Indigo
	{153, 177, 251}: 22, // Light Indigo
	{74, 66, 132}:   47, // Dark Slate Blue
	{122, 113, 196}: 48, // Slate Blue
	{181, 174, 241}: 49, // Light Slate Blue
	{120, 12, 153}:  23, // Dark Purple
	{170, 56, 185}:  24, // Purple
	{224, 159, 249}: 25, // Light Purple
	{203, 0, 122}:   26, // Dark Pink
	{236, 31, 128}:  27, // Pink
	{243, 141, 169}: 28, // Light Pink
	{155, 82, 73}:   53, // Dark Peach
	{209, 128, 120}: 54, // Peach
	{250, 182, 164}: 55, // Light Peach
	{104, 70, 52}:   29, // Dark Brown
	{149, 104, 42}:  30, // Brown
	{219, 164, 99}:  50, // Light Brown
	{123, 99, 82}:   56, // Dark Tan
	{156, 132, 107}: 57, // Tan
	{214, 181, 148}: 36, // Light Tan
	{209, 128, 81}:  51, // Dark Beige
	{248, 178, 119}: 31, // Beige
	{255, 197, 165}: 52, // Light Beige
	{109, 100, 63}:  61, // Dark Stone
	{148, 140, 107}: 62, // Stone
	{205, 197, 158}: 63, // Light Stone
	{51, 57, 65}:    58, // Dark Slate
	{109, 117, 141}: 59, // Slate
	{179, 185, 209}: 60, // Light Slate
	//{0, 0, 0}:       0,  // Transparent (special, not listed to avoid conflicts)
}

type Paletter struct {
	palette          []color.Color
	compressionLevel png.CompressionLevel
}

func NewPaletter() Paletter {
	p := Paletter{
		palette:          make([]color.Color, 64),
		compressionLevel: png.DefaultCompression, // BestCompression is 9x slower. Best speed is 4x faster but at significantly worse compression ratio
	}
	p.buildPalette()
	return p
}

func (p Paletter) buildPalette() {
	// Build palette from colorToIndex
	for rgb, idx := range colorToIndex {
		if idx == 0 {
			p.palette[idx] = color.RGBA{0, 0, 0, 0} // Transparent
		} else if idx >= 0 && idx < len(p.palette) {
			p.palette[idx] = color.RGBA{rgb[0], rgb[1], rgb[2], 255}
		}
	}

	// Fill empty palette slots with transparent black
	for i := range p.palette {
		if p.palette[i] == nil {
			p.palette[i] = color.RGBA{0, 0, 0, 0}
		}
	}
}

func (p Paletter) SwapPalette(img *image.Paletted) *image.Paletted {
	// Build a map from input palette index to target palette index
	indexMap := make(map[uint8]uint8)
	inPalette := img.Palette
	for i, c := range inPalette {
		r, g, b, a := c.RGBA()
		var idx int
		if a == 0 {
			idx = 0 // Transparent
		} else {
			r8 := uint8(r >> 8)
			g8 := uint8(g >> 8)
			b8 := uint8(b >> 8)
			var ok bool
			idx, ok = colorToIndex[[3]uint8{r8, g8, b8}]
			if !ok {
				fmt.Printf("Unknown color: %d %d %d\n", r8, g8, b8)
				idx = 0 // Unknown color -> transparent
			}
		}
		indexMap[uint8(i)] = uint8(idx)
	}

	// Create new paletted image with target palette
	outImg := image.NewPaletted(img.Bounds(), p.palette)
	for i := range len(img.Pix) {
		outImg.Pix[i] = indexMap[img.Pix[i]]
	}
	return outImg
}

func (p Paletter) RGBAToPalette(img image.Image) image.Image {
	bounds := img.Bounds()
	outImg := image.NewPaletted(bounds, p.palette)

	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, a := img.At(x, y).RGBA()
			var idx int
			if a == 0 {
				idx = 0 // Transparent -> 0
			} else {
				r8 := uint8(r >> 8)
				g8 := uint8(g >> 8)
				b8 := uint8(b >> 8)
				var ok bool
				idx, ok = colorToIndex[[3]uint8{r8, g8, b8}]
				if !ok {
					fmt.Printf("Unknown color: %d %d %d\n", r8, g8, b8)
					idx = 0 // Unknown color -> 0
				}
			}
			outImg.SetColorIndex(x, y, uint8(idx))
		}
	}
	return outImg
}

func (p Paletter) ToPalette(img image.Image) image.Image {
	switch img := img.(type) {
	case *image.Paletted:
		// Sligtly faster
		return p.SwapPalette(img)
	default:
		return p.RGBAToPalette(img)
	}
}

func (p Paletter) PngPack(img image.Image, out io.Writer) error {
	outImg := p.ToPalette(img)

	enc := png.Encoder{
		CompressionLevel: p.compressionLevel,
	}
	if err := enc.Encode(out, outImg); err != nil {
		return err
	}
	return nil
}

// EmptyImage produces a 1000x1000 png of alpha=0
func EmptyImage() []byte {
	img := image.NewRGBA(image.Rect(0, 0, 1000, 1000))
	for x := 0; x < 1000; x++ {
		for y := 0; y < 1000; y++ {
			img.Set(x, y, color.Transparent)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil
	}
	return buf.Bytes()
}

func EmptyImagePaletted(size int) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, size, size))
	for x := 0; x < size; x++ {
		for y := 0; y < size; y++ {
			img.Set(x, y, color.Transparent)
		}
	}
	p := NewPaletter()
	return p.ToPalette(img)
}

func EncodePng(i image.Image) ([]byte, error) {
	enc := png.Encoder{
		CompressionLevel: png.DefaultCompression,
	}
	var buf bytes.Buffer
	err := enc.Encode(&buf, i)
	return buf.Bytes(), err
}

func DecodeImage(data []byte) (image.Image, error) {
	i, _, err := image.Decode(bytes.NewReader(data))
	return i, err
}

func DecodePaletted(data []byte) (*image.Paletted, error) {
	i, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	ip, ok := i.(*image.Paletted)
	if !ok {
		return nil, fmt.Errorf("image not paletted")
	}
	return ip, nil
}
