package testutil

import (
	"bufio"
	"encoding/json"
	"github.com/guregu/null/v5"
	"io"
)

type LogRecord struct {
	LevelName  string                 `json:"level_name"`
	DateTime   null.Time              `json:"datetime"`
	Caller     string                 `json:"caller"`
	Message    string                 `json:"message"`
	Handler    string                 `json:"handler,omitempty"`
	Connection string                 `json:"connection,omitempty"`
	Account    string                 `json:"account,omitempty"`
	Context    map[string]interface{} `json:"context,omitempty"`
}

type JSONRecordScanner struct {
	r *bufio.Scanner
	e LogRecord
}

func NewJSONRecordScanner(entryProvider io.Reader) *JSONRecordScanner {
	return &JSONRecordScanner{r: bufio.NewScanner(entryProvider)}
}

func (s *JSONRecordScanner) Scan() error {
	if s.r.Scan() {
		return json.Unmarshal(s.r.Bytes(), &s.e)
	}
	return io.EOF
}

func (s *JSONRecordScanner) ScanAll() ([]LogRecord, error) {
	var entries []LogRecord
	for s.r.Scan() {
		entry := LogRecord{}
		if err := json.Unmarshal(s.r.Bytes(), &entry); err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *JSONRecordScanner) Entry() LogRecord {
	return s.e
}
