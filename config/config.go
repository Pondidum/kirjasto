package config

import (
	"context"
)

type Config struct {
}

func CreateConfig(ctx context.Context) (*Config, error) {
	return &Config{}, nil
}
