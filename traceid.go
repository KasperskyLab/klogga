package klogga

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type TraceID uuid.UUID

const TraceIDSize = 16

func NewTraceID() TraceID {
	return TraceID(uuid.New())
}

func (t TraceID) IsZero() bool {
	return t == TraceID{}
}

func (t TraceID) AsUUID() uuid.UUID {
	return uuid.UUID(t)
}

func (t TraceID) Bytes() []byte {
	return t[:]
}

func (t TraceID) AsNullableUUID() *uuid.UUID {
	if t.IsZero() {
		return nil
	}
	u := uuid.UUID(t)
	return &u
}

func TraceIDFromBytes(bb []byte) (TraceID, error) {
	if len(bb) != TraceIDSize {
		return TraceID{}, errors.Errorf("TraceIDFromString: wrong TraceID size %v expected %v", len(bb), TraceIDSize)
	}
	res, err := uuid.ParseBytes(bb)
	return TraceID(res), err
}

func TraceIDFromBytesOrZero(bb []byte) TraceID {
	res, err := uuid.ParseBytes(bb)
	if err != nil {
		return TraceID{}
	}
	return TraceID(res)
}

func TraceIDFromString(traceID string) (res TraceID, err error) {
	bytes, err := base64.RawURLEncoding.DecodeString(traceID)
	if err != nil {
		return TraceID{}, err
	}
	if len(bytes) == 0 {
		return TraceID{}, nil
	}
	if len(bytes) != len(res) {
		return TraceID{}, errors.Errorf("TraceIDFromString: wrong TraceID size %v expected %v", len(bytes), TraceIDSize)
	}
	copy(res[:], bytes)
	return res, err
}

// default storage and string format for TraceID is base64!
func (t TraceID) String() string {
	if t.IsZero() {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(t[:])
}

func (t TraceID) MarshalText() ([]byte, error) {
	if t.IsZero() {
		return nil, nil
	}
	return []byte(base64.RawURLEncoding.EncodeToString(t[:])), nil
}

func (t TraceID) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t *TraceID) UnmarshalJSON(bb []byte) error {
	var str string
	if err := json.Unmarshal(bb, &str); err != nil {
		return err
	}
	tt, err := TraceIDFromString(str)
	if err != nil {
		return err
	}
	*t = tt
	return err
}

// SpanID like in OpenTelemetry
type SpanID [8]byte

const SpanIDSize = 8

func (s SpanID) IsZero() bool {
	return s == SpanID{}
}

func NewSpanID() (res SpanID) {
	_, _ = rand.Read(res[:])
	return res
}

// default string format for SpanID is base64!
func (s SpanID) String() string {
	if s.IsZero() {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(s[:])
}

func (s SpanID) Bytes() []byte {
	return s[:]
}

func (s SpanID) AsNullableBytes() []byte {
	if s.IsZero() {
		return nil
	}
	return s[:]
}

func ParseSpanID(spanID string) (res SpanID, err error) {
	bytes, err := base64.RawURLEncoding.DecodeString(spanID)
	if err != nil {
		return SpanID{}, err
	}
	if len(bytes) != SpanIDSize {
		return SpanID{}, errors.Errorf("ParseSpanID: wrong SpanID size %v expected %v", len(bytes), SpanIDSize)
	}
	copy(res[:], bytes)
	return res, err
}

func (s SpanID) MarshalText() ([]byte, error) {
	if s.IsZero() {
		return nil, nil
	}
	return []byte(s.String()), nil
}

func (s SpanID) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *SpanID) UnmarshalJSON(bb []byte) error {
	var str string
	if err := json.Unmarshal(bb, &str); err != nil {
		return err
	}
	tt, err := SpanIDFromString(str)
	if err != nil {
		return err
	}
	*s = tt
	return err
}

func SpanIDFromBytes(bb []byte) (res SpanID, err error) {
	if len(bb) != SpanIDSize {
		return SpanID{}, errors.Errorf("SpanIDFromBytes: wrong SpanID size %v expected %v", len(bb), SpanIDSize)
	}
	copy(res[:], bb)
	return res, nil
}

func SpanIDFromBytesOrZero(bb []byte) SpanID {
	res, err := SpanIDFromBytes(bb)
	if err != nil {
		return SpanID{}
	}
	return res
}

func SpanIDFromString(s string) (res SpanID, err error) {
	bytes, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return SpanID{}, err
	}
	if len(bytes) == 0 {
		return SpanID{}, nil
	}
	if len(bytes) != SpanIDSize {
		return SpanID{}, errors.Errorf("SpanIDFromString: wrong SpanID size %v expected %v", len(bytes), SpanIDSize)
	}
	copy(res[:], bytes)
	return res, err
}
