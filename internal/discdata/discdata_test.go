package discdata

import (
	"fmt"
	"os"
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
