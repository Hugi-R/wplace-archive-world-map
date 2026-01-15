package img

import (
	"bytes"
	"fmt"
	"image"
	"os"
	"reflect"
	"testing"
)

func loadImageT(path string, t *testing.T) image.Image {
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	img, _, err := image.Decode(file)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

func TestImgPack(t *testing.T) {
	p := NewPaletter()
	im1 := loadImageT("testdata/tiles-146_0-0.png", t)
	expected := loadImageT("testdata/result_tiles-146-0-0.png", t)

	t.Run("CorrectPalette", func(t *testing.T) {
		packed := bytes.Buffer{}
		p.PngPack(im1, &packed)
		res, err := DecodeImage(packed.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		pRes, ok := res.(*image.Paletted)
		if !ok {
			t.Fatal("res is not paletted")
		}
		pExp, ok := expected.(*image.Paletted)
		if !ok {
			t.Fatal("exp is not paletted")
		}
		if !reflect.DeepEqual(pExp.Palette, pRes.Palette) {
			t.Fail()
		}
		if !reflect.DeepEqual(pExp.Pix, pRes.Pix) {
			t.Fail()
		}

	})

}

func BenchmarkImgPack(b *testing.B) {
	p := NewPaletter()
	im1 := loadImageB("testdata/tiles-146_0-0.png", b)

	b.Run("SwapPalette", func(b *testing.B) {
		for b.Loop() {
			switch im := im1.(type) {
			case *image.Paletted:
				p.SwapPalette(im)
			default:
				panic("benchmarked image is not paletted")
			}
		}
	})

	b.Run("RGBAToPalette", func(b *testing.B) {
		for b.Loop() {
			p.RGBAToPalette(im1)
		}
	})

	var packed bytes.Buffer
	b.Run("PngPack", func(b *testing.B) {
		for b.Loop() {
			packed = bytes.Buffer{}
			p.PngPack(im1, &packed)
		}
	})
	fmt.Fprintf(os.Stdout, "Img packed size: %d kiB\n", len(packed.Bytes())/1024)
}
