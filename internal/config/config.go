package config

import (
	"flag"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	GrpcPort    string        `yaml:"grpc_port"`
	QueueSize   int           `yaml:"queue_size"`
	LogLevel    string        `yaml:"log_level"`
	SendTimeout time.Duration `yaml:"send_timeout"`
}

func ReadConfig() (*Config, error) {
	configPath := getConfigPath()
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	if err := yaml.Unmarshal(configData, config); err != nil {
		return nil, err
	}

	return config, nil
}

func MustConfig() *Config {
	config, err := ReadConfig()
	if err != nil {
		panic(err)
	}

	return config
}

func getConfigPath() string {
	var configPath string
	flag.StringVar(&configPath, "config_path", "", "path to config")
	flag.Parse()

	if configPath == "" {
		configPath = os.Getenv("CONFIG_PATH")
	}

	return configPath
}
