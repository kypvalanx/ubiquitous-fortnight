package convert

import "fmt"

type EncodingPreset struct {
	Name string
	Args []string
}

var ArchivePreset = EncodingPreset{
	Name: "archive",
	Args: []string{
		"--encoder", "nvenc_h265",
		"--quality", "18",
		"--encoder-preset", "slow",

		"--vfr",

		"--all-audio",
		"--audio-copy-mask", "truehd,dtshd,eac3,dts,ac3,aac,flac",
		"--audio-fallback", "ac3",

		"--all-subtitles",

		"--markers",
	},
}

var DefaultPreset = EncodingPreset{
	Name: "default",
	Args: []string{
		"--encoder", "nvenc_h265",
		"--quality", "20",
		"--encoder-preset", "medium",

		"--vfr",

		"--all-audio",
		"--audio-copy-mask", "truehd,dtshd,eac3,dts,ac3,aac",
		"--audio-fallback", "ac3",

		"--all-subtitles",

		"--markers",
	},
}

var DVDPreset = EncodingPreset{
	Name: "dvd",
	Args: []string{
		"--encoder", "nvenc_h265",
		"--quality", "19",
		"--encoder-preset", "slow",

		"--rate", "same",

		"--all-audio",
		"--audio-copy-mask", "ac3,dts,aac",
		"--audio-fallback", "ac3",

		"--all-subtitles",

		"--comb-detect",
		"--decomb",

		"--markers",
	},
}

var TVPreset = EncodingPreset{
	Name: "tv",
	Args: []string{
		"--encoder", "nvenc_h265",
		"--quality", "22",
		"--encoder-preset", "fast",

		"--vfr",

		"--all-audio",
		"--audio-copy-mask", "ac3,eac3,aac",
		"--audio-fallback", "aac",

		"--all-subtitles",

		"--markers",
	},
}
var Presets = map[string]EncodingPreset{
	ArchivePreset.Name: ArchivePreset,
	DefaultPreset.Name: DefaultPreset,
	DVDPreset.Name:     DVDPreset,
	TVPreset.Name:      TVPreset,
}

const (
	PresetArchive = "archive"
	PresetDefault = "default"
	PresetDVD     = "dvd"
	PresetTV      = "tv"
)

func GetPreset(name string) (EncodingPreset, error) {
	preset, ok := Presets[name]
	if !ok {
		return EncodingPreset{}, fmt.Errorf("unknown preset: %s", name)
	}

	return preset, nil
}
