package dbflex

import (
	"errors"

	"github.com/eaciit/toolkit"
)

var log *toolkit.LogEngine

// EOF is the error returned by dbflex when document/data/row not found.
// This EOF should be different with io.EOF
// You can use for data not found
var EOF = errors.New("EOF")

func Logger() *toolkit.LogEngine {
	if log == nil {
		log, _ = toolkit.NewLog(true, false, "", "", "")
	}
	return log
}

func SetLogger(l *toolkit.LogEngine) {
	log = l
}
