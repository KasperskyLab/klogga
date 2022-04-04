package klogga

import (
	"encoding/base64"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

type TraceId uuid.UUID

func NewTraceId() TraceId {
	return TraceId(uuid.NewV4())
}

func (t TraceId) IsZero() bool {
	return uuid.UUID(t) == uuid.Nil
}

func (t TraceId) Bytes() []byte {
	return uuid.UUID(t).Bytes()
}

func (t TraceId) AsUUID() uuid.UUID {
	return uuid.UUID(t)
}

func (t TraceId) AsNullableUUID() *uuid.UUID {
	if t.IsZero() {
		return nil
	}
	u := uuid.UUID(t)
	return &u
}

func TraceIdFromBytes(bb []byte) (res TraceId, err error) {
	if len(bb) != uuid.Size {
		return TraceId{}, errors.Errorf("traceID must be %v long, got %v", uuid.Size, len(bb))
	}
	copy(res[:], bb)
	return res, nil
}

func TraceIdFromBytesOrZero(bb []byte) TraceId {
	res, err := TraceIdFromBytes(bb)
	if err != nil {
		return TraceId{}
	}
	return res
}

func ParseTraceId(traceId string) (res TraceId, err error) {
	bytes, err := base64.RawURLEncoding.DecodeString(traceId)
	if err != nil {
		return TraceId{}, err
	}
	if len(bytes) != uuid.Size {
		return TraceId{}, errors.Errorf("wrong traceId size %v expected %v", len(bytes), uuid.Size)
	}
	copy(res[:], bytes)
	return res, err
}

// default storage and string format for TraceId is base64!
func (t TraceId) String() string {
	if t.IsZero() {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(t[:])
}

func (t TraceId) MarshalText() ([]byte, error) {

	if str := t.String(); str == "" {
		return nil, nil
	}
	return []byte(base64.RawURLEncoding.EncodeToString(t[:])), nil
}

//func (t TraceId) MarshalJSON() ([]byte, error) {
//	str := t.String()
//	if str == "" {
//		return nil, nil
//	}
//	return json.Marshal([]byte(base64.RawURLEncoding.EncodeToString(t[:])))
//}
