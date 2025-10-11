package img

import (
	"fmt"
	"image"
	"reflect"
)

func Diff(base []byte, new []byte) ([]byte, bool, error) {
	baseI, err := DecodeImage(base)
	if err != nil {
		return nil, false, err
	}
	newI, err := DecodeImage(new)
	if err != nil {
		return nil, false, err
	}

	baseP, ok := baseI.(*image.Paletted)
	if !ok {
		return nil, false, fmt.Errorf("input image base is not paletted")
	}
	newP, ok := newI.(*image.Paletted)
	if !ok {
		return nil, false, fmt.Errorf("input image new is not paletted")
	}

	diff, changes, err := DiffPaletted(baseP, newP)
	if err != nil {
		return nil, false, err
	}
	diffData, err := EncodePng(diff)
	if err != nil {
		return nil, false, err
	}

	return diffData, changes, nil
}

func DiffPaletted(base *image.Paletted, new *image.Paletted) (*image.Paletted, bool, error) {
	// Check palettes are the same
	if !reflect.DeepEqual(base.Palette, new.Palette) {
		return nil, false, fmt.Errorf("input images base and new have different palettes")
	}
	// Check size
	if len(base.Pix) != len(new.Pix) {
		return nil, false, fmt.Errorf("input images differ in size")
	}

	diff := image.NewPaletted(base.Rect, base.Palette)

	changes := false
	for i := range len(base.Pix) {
		if base.Pix[i] != new.Pix[i] {
			diff.Pix[i] = new.Pix[i]
			changes = true
		} else {
			diff.Pix[i] = 0 // transparent, see imgpack.go
		}
	}

	return diff, changes, nil
}

func UnDiffPaletted(base *image.Paletted, new *image.Paletted) (*image.Paletted, error) {
	// Check palettes are the same
	if !reflect.DeepEqual(base.Palette, new.Palette) {
		return nil, fmt.Errorf("input images base and new have different palettes")
	}
	// Check size
	if len(base.Pix) != len(new.Pix) {
		return nil, fmt.Errorf("input images differ in size")
	}
	undiff := image.NewPaletted(base.Rect, base.Palette)

	for i := range len(base.Pix) {
		if new.Pix[i] == 0 {
			// if new pix is transparent, use base
			undiff.Pix[i] = base.Pix[i]
		} else {
			// else use new
			undiff.Pix[i] = new.Pix[i]
		}
	}

	return undiff, nil
}
