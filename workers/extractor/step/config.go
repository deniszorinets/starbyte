package extractor

type ExtractField struct {
	CastTo     string `json:"cast_to"  mapstructure:"cast_to"`
	JsonPath   string `json:"json_path"  mapstructure:"json_path"`
	DateFormat string `json:"date_format,omitempty"  mapstructure:"date_format"`
}

type ExtractorConfig struct {
	Columns map[string]ExtractField `json:"columns" mapstructure:"columns"`
}
