package kafka

//type TopicType string

type TopicType struct {
	Name        string
	Description string
}

var (
	DiscDiscovered = TopicType{
		Name:        "disc.discovered",
		Description: "Discovered",
	}
	DiscConverted = TopicType{
		Name:        "disc.converted",
		Description: "Converted",
	}
	DiscConvertProgress = TopicType{
		Name:        "disc.convert.progress",
		Description: "Convert Progress",
	}
	DiscMetadata = TopicType{
		Name:        "disc.metadata",
		Description: "Metadata",
	}
	DiscRipped = TopicType{
		Name:        "disc.ripped",
		Description: "Ripped",
	}
	DiscRipProgress = TopicType{
		Name:        "disc.rip.progress",
		Description: "Rip Progress",
	}
	DiscData = TopicType{
		Name:        "disc.discdata",
		Description: "DiscData",
	}
	DiscTitlesSelected = TopicType{
		Name:        "disc.titles.selected",
		Description: "TitlesSelected",
	}
	DiscTitlesAmbiguous = TopicType{
		Name: "disc.titles.ambiguous",
	}
)

var Topics = []TopicType{
	DiscDiscovered,
	DiscMetadata,
	DiscRipped,
	DiscRipProgress,
	DiscConverted,
	DiscConvertProgress,
}
