package discdata

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestService_ParseMakeMKVOutput(t *testing.T) {
	data, err := os.ReadFile("testdata/bluray/holygrail.txt")
	if err != nil {
		t.Fatal(err)
	}

	discInfo, err := ParseMakeMKVOutput(string(data))

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(discInfo)
}
func TestService_ParseMakeMKVOutput_bsgS02D01(t *testing.T) {
	data, err := os.ReadFile("testdata/bluray/BSG.txt")
	if err != nil {
		t.Fatal(err)
	}

	discInfo, err := ParseMakeMKVOutput(string(data))

	if err != nil {
		fmt.Println(err)
	}

	for _, title := range discInfo.Titles {
		fmt.Println(strconv.Itoa(title.ID) + " : " + fmt.Sprintf("%f", title.Duration.Minutes()))
	}
	fmt.Println(discInfo)
}

func TestService_ParseMakeMKVOutput_GGS01D01(t *testing.T) {
	data, err := os.ReadFile("testdata/bluray/GG_S1_D1.txt")
	if err != nil {
		t.Fatal(err)
	}

	discInfo, err := ParseMakeMKVOutput(string(data))

	if err != nil {
		fmt.Println(err)
	}

	for _, title := range discInfo.Titles {
		fmt.Println(strconv.Itoa(title.ID) + " : " + fmt.Sprintf("%f", title.Duration.Minutes()))
	}
	fmt.Println(discInfo.Label)
}

func TestService_ParseMakeMKVOutput_GGS01D02(t *testing.T) {
	data, err := os.ReadFile("testdata/bluray/GG_S1_D2.txt")
	if err != nil {
		t.Fatal(err)
	}

	discInfo, err := ParseMakeMKVOutput(string(data))

	if err != nil {
		fmt.Println(err)
	}

	for _, title := range discInfo.Titles {
		fmt.Println(strconv.Itoa(title.ID) + " : " + fmt.Sprintf("%f", title.Duration.Minutes()))
	}
	fmt.Println(discInfo)
}
