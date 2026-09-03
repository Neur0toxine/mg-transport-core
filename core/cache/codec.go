package cache

import (
	"bytes"
	"encoding/base64"
	json "encoding/json/v2"
)

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

type JSONCodec[T any] struct{}

func (JSONCodec[T]) Encode(value T) ([]byte, error) {
	return json.Marshal(value)
}

func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

type BytesCodec struct{}

func (BytesCodec) Encode(value []byte) ([]byte, error) {
	return bytes.Clone(value), nil
}

func (BytesCodec) Decode(data []byte) ([]byte, error) {
	return bytes.Clone(data), nil
}

type KeyEncoder[K comparable] interface {
	EncodeKey(K) (string, error)
}

type StringKeyEncoder struct{}

func (StringKeyEncoder) EncodeKey(key string) (string, error) {
	return key, nil
}

type JSONKeyEncoder[K comparable] struct{}

func (JSONKeyEncoder[K]) EncodeKey(key K) (string, error) {
	data, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}
