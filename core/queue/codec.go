package queue

import json "encoding/json/v2"

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

type JSONCodec[T any] struct{}

func (JSONCodec[T]) Encode(value T) ([]byte, error) { return json.Marshal(value) }

func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

type BytesCodec struct{}

func (BytesCodec) Encode(value []byte) ([]byte, error) { return append([]byte(nil), value...), nil }
func (BytesCodec) Decode(data []byte) ([]byte, error)  { return append([]byte(nil), data...), nil }
