package config

import (
	"github.com/caarlos0/env/v10"
	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type data struct {
	KafkaURL     string `env:"KAFKA_URL" envDefault:"localhost:9092" validate:"uri"`
	KafkaTopic   string `env:"KAFKA_TOPIC" envDefault:"prices" validate:"uri"`
	RpcChartPort string `env:"RPC_CHART_PORT" envDefault:"localhost:7071" validate:"uri"`
	RpcPosPort   string `env:"RPC_POS_PORT" envDefault:"localhost:7073" validate:"uri"`
	RedisURL     string `env:"REDIS_URI" envDefault:"redis://localhost:6379/0?protocol=3" validate:"uri"`
}

func New() (data, error) {
	conf := data{}
	if err := env.Parse(&conf); err != nil {
		log.Errorf("Parse config error: %v\n", err)
	}
	if err := validator.New(validator.WithRequiredStructEnabled()).Struct(&conf); err != nil {
		return data{}, err
	}

	return conf, nil
}
