package config

import (
	"github.com/caarlos0/env/v10"
	log "github.com/sirupsen/logrus"
)

type data struct {
	KafkaURL   string `env:"KAFKA_URL" envDefault:"localhost:9092"`
	KafkaTopic string `env:"KAFKA_TOPIC" envDefault:"prices"`
}

func New() data {
	conf := data{}
	if err := env.Parse(&conf); err != nil {
		log.Errorf("Parse config error: %v\n", err)
	}

	return conf
}
