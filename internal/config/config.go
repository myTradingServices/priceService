package config

import (
	"os"

	"github.com/caarlos0/env/v10"
	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type data struct {
	KafkaURL     string `env:"KAFKA_URL" envDefault:"localhost:9092" validate:"uri"`
	KafkaTopic   string `env:"KAFKA_TOPIC" envDefault:"prices"`
	RpcChartPort string `env:"RPC_CHART_PORT" envDefault:"7071"`
	RpcPosPort   string `env:"RPC_POS_PORT" envDefault:"7073"`
	RedisURL     string `env:"REDIS_URL" envDefault:"redis://localhost:6379/0?protocol=3" validate:"uri"`
}

func New() (data, error) {
	conf := data{}
	if err := env.Parse(&conf); err != nil {
		log.Errorf("Parse config error: %v\n", err)
		return data{}, err
	}
	if err := validator.New(validator.WithRequiredStructEnabled()).Struct(&conf); err != nil {
		log.Errorf("Validate config error: %v\n", err)
		return data{}, err
	}

	log.WithFields(log.Fields{
		"input kafka_url":      conf.KafkaURL,
		"system env KAFKA_URL": os.Getenv("KAFKA_URL"),
		"input redis_url":      conf.RedisURL,
		"system env REDIS_URL": os.Getenv("REDIS_URL"),
	}).Info("Input config from config.go:\n")

	return conf, nil
}
