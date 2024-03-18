package config

import (
	"github.com/caarlos0/env/v10"
	log "github.com/sirupsen/logrus"
)

type data struct {
	KafkaURL     string `env:"KAFKA_URL" envDefault:"localhost:9092"`
	KafkaTopic   string `env:"KAFKA_TOPIC" envDefault:"prices"`
	RpcChartPort string `env:"RPC_CHART_PORT" envDefault:"localhost:7071"`
	RpcPosPort   string `env:"RPC_POS_PORT" envDefault:"localhost:7073"`
}

func New() data {
	conf := data{}
	if err := env.Parse(&conf); err != nil {
		log.Errorf("Parse config error: %v\n", err)
	}

	return conf
}
