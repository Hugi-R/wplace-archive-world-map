package img

import (
	"image"
	"os"
	"testing"
)

func loadImageB(path string, b *testing.B) image.Image {
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	img, _, err := image.Decode(file)
	if err != nil {
		b.Fatal(err)
	}
	return img
}

func loadFile(path string, b *testing.B) []byte {
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	return data
}

func BenchmarkLoadImage(b *testing.B) {
	b.Run("GoLoadImage", func(b *testing.B) {
		for b.Loop() {
			loadImageB("testdata/tile-v2-11-1036-704.png", b)
		}
	})
}

func BenchmarkFastResizeAndMerge(b *testing.B) {
	b.Run("FastAvgResize2", func(b *testing.B) {
		i1 := loadImageB("testdata/tile-v2-11-1036-704.png", b)
		i2 := loadImageB("testdata/tile-v2-11-1037-704.png", b)
		i3 := loadImageB("testdata/tile-v2-11-1036-705.png", b)
		i4 := loadImageB("testdata/tile-v2-11-1037-705.png", b)
		for b.Loop() {
			FastResizeAndMerge(i1, i2, i3, i4, FastAvgResize2)
		}
	})
	b.Run("FastResize2", func(b *testing.B) {
		i1 := loadImageB("testdata/tile-v2-11-1036-704.png", b)
		i2 := loadImageB("testdata/tile-v2-11-1037-704.png", b)
		i3 := loadImageB("testdata/tile-v2-11-1036-705.png", b)
		i4 := loadImageB("testdata/tile-v2-11-1037-705.png", b)
		for b.Loop() {
			FastResizeAndMerge(i1, i2, i3, i4, FastResize2)
		}
	})
	b.Run("FastPaletteResize2", func(b *testing.B) {
		i1 := loadImageB("testdata/tile-v2-11-1036-704.png", b)
		i2 := loadImageB("testdata/tile-v2-11-1037-704.png", b)
		i3 := loadImageB("testdata/tile-v2-11-1036-705.png", b)
		i4 := loadImageB("testdata/tile-v2-11-1037-705.png", b)
		for b.Loop() {
			_, err := FastPalettedResizeAndMerge(i1, i2, i3, i4, FastPaletteResize2)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
