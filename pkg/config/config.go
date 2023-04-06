package config

import (
	"bytes"
	"io"
	"os"

	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v3"
)

type Config struct {
	Topic   string   `yaml:"topic"`
	Workers int      `yaml:"workers"`
	Brokers []string `yaml:"brokers"`
}

func GetConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening config from %v", path)
	}
	defer f.Close()

	buff := new(bytes.Buffer)
	if _, err := io.Copy(buff, f); err != nil {
		return nil, errors.Wrapf(err, "error reading file")
	}
	c := &Config{}
	if err := yaml.Unmarshal(buff.Bytes(), c); err != nil {
		return nil, errors.Wrapf(err, "error unpacking file to configuration")
	}

	return c, nil
}
