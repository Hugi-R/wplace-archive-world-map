package img

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"reflect"
)

func MergeAndResize(positions map[[2]int][]byte, block int) (image.Image, error) {
	// Validate input
	if len(positions) != block*block {
		return nil, fmt.Errorf("invalid number of images to merge, got: %d, want: %d", len(positions), block*block)
	}

	imgW, imgH := 1000, 1000
	canvasW := block * imgW
	canvasH := block * imgH
	canvas := image.NewRGBA(image.Rect(0, 0, canvasW, canvasH))

	for pos, data := range positions {
		img, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		x := pos[0] * imgW
		y := pos[1] * imgH
		for i := 0; i < imgW; i++ {
			for j := 0; j < imgH; j++ {
				canvas.Set(x+i, y+j, img.At(img.Bounds().Min.X+i, img.Bounds().Min.Y+j))
			}
		}
	}

	// Resize merged image using majority vote (mode color)
	targetW, targetH := canvasW/block, canvasH/block
	resized := image.NewRGBA(image.Rect(0, 0, targetW, targetH))
	for y := 0; y < targetH; y++ {
		for x := 0; x < targetW; x++ {
			colorCount := make(map[uint64]int)
			maxCount := 0
			var modeColor color.Color
			for by := 0; by < block; by++ {
				for bx := 0; bx < block; bx++ {
					px := x*block + bx
					py := y*block + by
					r, g, b, a := canvas.At(px, py).RGBA()
					key := (uint64(r) << 48) | (uint64(g) << 32) | (uint64(b) << 16) | uint64(a)
					colorCount[key]++
					if a != 0 {
						colorCount[key] += block - 1
					}
					if colorCount[key] > maxCount {
						maxCount = colorCount[key]
						modeColor = canvas.At(px, py)
					}
				}
			}
			resized.Set(x, y, modeColor)
		}
	}
	return resized, nil
}

// Image layout:
// A B
// C D
// Assuming all images are the same size
func FastResizeAndMerge(a, b, c, d image.Image, resizeFunc func(image.Image) image.Image) image.Image {
	ra := resizeFunc(a)
	rb := resizeFunc(b)
	rc := resizeFunc(c)
	rd := resizeFunc(d)

	imgW, imgH := a.Bounds().Dx(), a.Bounds().Dy()
	canvasW := imgW * 2
	canvasH := imgH * 2
	canvas := image.NewRGBA(image.Rect(0, 0, canvasW, canvasH))

	// Draw images onto canvas
	for y := 0; y < imgH; y++ {
		for x := 0; x < imgW; x++ {
			canvas.Set(x, y, ra.At(x, y))
			canvas.Set(x+imgW, y, rb.At(x, y))
			canvas.Set(x, y+imgH, rc.At(x, y))
			canvas.Set(x+imgW, y+imgH, rd.At(x, y))
		}
	}
	return canvas
}

func FastResize2(img image.Image) image.Image {
	srcBounds := img.Bounds()
	dstW := srcBounds.Dx() / 2
	dstH := srcBounds.Dy() / 2
	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))

	for y := range dstH {
		for x := range dstW {
			r, g, b, a := img.At(x*2+0, y*2+0).RGBA()
			c0 := (uint64(r) << 48) | (uint64(g) << 32) | (uint64(b) << 16) | uint64(a)
			r, g, b, a = img.At(x*2+1, y*2+0).RGBA()
			c1 := (uint64(r) << 48) | (uint64(g) << 32) | (uint64(b) << 16) | uint64(a)
			r, g, b, a = img.At(x*2+0, y*2+1).RGBA()
			c2 := (uint64(r) << 48) | (uint64(g) << 32) | (uint64(b) << 16) | uint64(a)
			r, g, b, a = img.At(x*2+1, y*2+1).RGBA()
			c3 := (uint64(r) << 48) | (uint64(g) << 32) | (uint64(b) << 16) | uint64(a)
			most := MostNonTransparentColor2x2(c0, c1, c2, c3)
			r = uint32((most >> 48) & 0xFFFF)
			g = uint32((most >> 32) & 0xFFFF)
			b = uint32((most >> 16) & 0xFFFF)
			a = uint32(most & 0xFFFF)
			dst.Set(x, y, color.RGBA{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8), uint8(a >> 8)})
		}
	}
	return dst
}

func MostFrequentColor2x2(a, b, c, d uint64) uint64 {
	// Check if any color appears 3+ times (majority)
	if a == b && b == c {
		return a
	}
	if a == b && b == d {
		return a
	}
	if a == c && c == d {
		return a
	}
	if b == c && c == d {
		return b
	}

	// Check if any color appears 2 times
	if a == b {
		return a
	}
	if a == c {
		return a
	}
	if a == d {
		return a
	}
	if b == c {
		return b
	}
	if b == d {
		return b
	}
	if c == d {
		return c
	}

	// All different, return first pixel
	return a
}

func MostNonTransparentColor2x2(a, b, c, d uint64) uint64 {
	// Helper to check alpha
	alpha := func(x uint64) uint16 { return uint16(x & 0xFFFF) }

	// Replace fully transparent pixels with sentinel
	a0 := alpha(a) == 0
	b0 := alpha(b) == 0
	c0 := alpha(c) == 0
	d0 := alpha(d) == 0

	// Return early if all transparent
	if a0 && b0 && c0 && d0 {
		return 0
	}

	// Majority logic, skipping transparent
	if !a0 && a == b && a == c {
		return a
	}
	if !a0 && a == b && a == d {
		return a
	}
	if !a0 && a == c && a == d {
		return a
	}
	if !b0 && b == c && b == d {
		return b
	}

	if !a0 && a == b {
		return a
	}
	if !a0 && a == c {
		return a
	}
	if !a0 && a == d {
		return a
	}
	if !b0 && b == c {
		return b
	}
	if !b0 && b == d {
		return b
	}
	if !c0 && c == d {
		return c
	}

	// Return first non-transparent pixel
	for _, v := range []uint64{a, b, c, d} {
		if alpha(v) != 0 {
			return v
		}
	}
	return 0 // fallback, should not reach here
}

func FastAvgResize2(img image.Image) image.Image {
	srcBounds := img.Bounds()
	dstW := srcBounds.Dx() / 2
	dstH := srcBounds.Dy() / 2
	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))

	for y := range dstH {
		for x := range dstW {
			r0, g0, b0, a0 := img.At(x*2+0, y*2+0).RGBA()
			r1, g1, b1, a1 := img.At(x*2+1, y*2+0).RGBA()
			r2, g2, b2, a2 := img.At(x*2+0, y*2+1).RGBA()
			r3, g3, b3, a3 := img.At(x*2+1, y*2+1).RGBA()
			r := (r0 + r1 + r2 + r3) / 4
			g := (g0 + g1 + g2 + g3) / 4
			b := (b0 + b1 + b2 + b3) / 4
			a := (a0 + a1 + a2 + a3) / 4
			dst.Set(x, y, color.RGBA{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8), uint8(a >> 8)})
		}
	}
	return dst
}

func FastPalettedResizeAndMerge(a, b, c, d image.Image, resizeFunc func(*image.Paletted, *image.Paletted, int, int)) (*image.Paletted, error) {
	ap, ok := a.(*image.Paletted)
	if !ok {
		return nil, fmt.Errorf("input image A is not paletted")
	}
	bp, ok := b.(*image.Paletted)
	if !ok {
		return nil, fmt.Errorf("input image B is not paletted")
	}
	cp, ok := c.(*image.Paletted)
	if !ok {
		return nil, fmt.Errorf("input image C is not paletted")
	}
	dp, ok := d.(*image.Paletted)
	if !ok {
		return nil, fmt.Errorf("input image D is not paletted")
	}

	// Check palettes are the same
	if !reflect.DeepEqual(ap.Palette, bp.Palette) {
		return nil, fmt.Errorf("input images A and B have different palettes")
	}
	if !reflect.DeepEqual(ap.Palette, cp.Palette) {
		return nil, fmt.Errorf("input images A and C have different palettes")
	}
	if !reflect.DeepEqual(ap.Palette, dp.Palette) {
		return nil, fmt.Errorf("input images A and D have different palettes")
	}

	canvasW, canvasH := a.Bounds().Dx(), a.Bounds().Dy()
	canvas := image.NewPaletted(image.Rect(0, 0, canvasW, canvasH), ap.Palette)

	halfW, halfH := canvasW/2, canvasH/2

	resizeFunc(ap, canvas, 0, 0)
	resizeFunc(bp, canvas, halfW, 0)
	resizeFunc(cp, canvas, 0, halfH)
	resizeFunc(dp, canvas, halfW, halfH)
	return canvas, nil
}

// func FastPaletteResize2Conv(img image.Image) image.Image {
// 	mp, ok := img.(*image.Paletted)
// 	if !ok {
// 		panic("input image is not paletted")
// 	}
// 	return FastPaletteResize2(mp)
// }

func FastPaletteResize2(in *image.Paletted, out *image.Paletted, xOffset, yOffset int) {
	srcBounds := in.Bounds()
	dstW := srcBounds.Dx() / 2
	dstH := srcBounds.Dy() / 2

	cA0 := in.Palette.Convert(color.RGBA{0, 0, 0, 0})
	iTransparent := in.Palette.Index(cA0)

	for x := range dstW {
		for y := range dstH {
			a := in.Pix[in.PixOffset(x*2+0, y*2+0)]
			b := in.Pix[in.PixOffset(x*2+1, y*2+0)]
			c := in.Pix[in.PixOffset(x*2+0, y*2+1)]
			d := in.Pix[in.PixOffset(x*2+1, y*2+1)]
			most := MostNonTransparentColor2x2Paletted(a, b, c, d, uint8(iTransparent))
			out.SetColorIndex(x+xOffset, y+yOffset, most)
		}
	}
}

func MostNonTransparentColor2x2Paletted(a, b, c, d uint8, transparent uint8) uint8 {
	// Replace fully transparent pixels with sentinel
	a0 := a == transparent
	b0 := b == transparent
	c0 := c == transparent
	d0 := d == transparent

	// Return early if all transparent
	if a0 && b0 && c0 && d0 {
		return 0
	}

	// Majority logic, skipping transparent
	if !a0 && a == b && a == c {
		return a
	}
	if !a0 && a == b && a == d {
		return a
	}
	if !a0 && a == c && a == d {
		return a
	}
	if !b0 && b == c && b == d {
		return b
	}

	if !a0 && a == b {
		return a
	}
	if !a0 && a == c {
		return a
	}
	if !a0 && a == d {
		return a
	}
	if !b0 && b == c {
		return b
	}
	if !b0 && b == d {
		return b
	}
	if !c0 && c == d {
		return c
	}

	// Return first non-transparent pixel
	for _, v := range []uint8{a, b, c, d} {
		if v != transparent {
			return v
		}
	}
	return 0 // fallback, should not reach here
}
