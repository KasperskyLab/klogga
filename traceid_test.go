package klogga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreateParseTraceID(t *testing.T) {
	id := NewTraceID()

	res, err := TraceIDFromString(id.String())
	require.NoError(t, err)
	require.Equal(t, id, res)
}

func TestJsonMarshalTraceID(t *testing.T) {
	id := NewTraceID()
	jsonStr, err := json.Marshal(&id)
	require.NoError(t, err)
	var unmarshalledID TraceID
	err = json.Unmarshal(jsonStr, &unmarshalledID)
	require.NoError(t, err)
	require.Equal(t, id, unmarshalledID)
}

func TestJsonMarshalTraceIDinStruct(t *testing.T) {
	id := NewTraceID()
	type St struct {
		ID   TraceID
		Some string
	}
	val := St{id, "lala"}

	jsonStr, err := json.Marshal(&val)
	require.NoError(t, err)
	var unmarshalledID St
	err = json.Unmarshal(jsonStr, &unmarshalledID)
	require.NoError(t, err)
	require.Equal(t, id, unmarshalledID.ID)
	t.Log("json:", string(jsonStr))
}

func TestCreateParseSpanID(t *testing.T) {
	id := NewSpanID()

	res, err := SpanIDFromString(id.String())
	require.NoError(t, err)
	require.Equal(t, id, res)
}

func TestJsonMarshalSpanID(t *testing.T) {
	id := NewSpanID()
	jsonStr, err := json.Marshal(&id)
	require.NoError(t, err)
	var unmarshalledID SpanID
	err = json.Unmarshal(jsonStr, &unmarshalledID)
	require.NoError(t, err)
	require.Equal(t, id, unmarshalledID)
}

func TestJsonMarshalSpanIDinStruct(t *testing.T) {
	id := NewSpanID()
	type St struct {
		Some string
		ID   SpanID
	}
	val := St{"lala", id}

	jsonStr, err := json.Marshal(&val)
	require.NoError(t, err)
	var unmarshalledID St
	err = json.Unmarshal(jsonStr, &unmarshalledID)
	require.NoError(t, err)
	require.Equal(t, id, unmarshalledID.ID)
}

func TestJsonMarshalIDsInStruct(t *testing.T) {
	idT, idS := TraceID{}, SpanID{}
	type St struct {
		ID      SpanID
		TraceID TraceID
	}
	val := St{idS, idT}

	jsonStr, err := json.Marshal(&val)
	require.NoError(t, err)
	var unmarshalledID St
	err = json.Unmarshal(jsonStr, &unmarshalledID)
	require.NoError(t, err)
	require.Equal(t, idS, unmarshalledID.ID)
	require.Equal(t, idT, unmarshalledID.TraceID)
	t.Log("json:", string(jsonStr))
}
