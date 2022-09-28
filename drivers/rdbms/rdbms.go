package rdbms

var timeFormat string

func TimeFormat() string {
	if timeFormat == "" {
		timeFormat = "yyyy-MM-dd hh:mm:ss"
	}
	return timeFormat
}

func (c *Cursor) SetTimeFormat(f string) {
	timeFormat = f
}
