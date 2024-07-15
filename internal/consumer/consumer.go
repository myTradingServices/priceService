package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/internal/service"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type consumer struct {
	brokerURL string
	topic     string
	chanels   map[string]chan model.Price
	redis     service.RedisCasher
}

type Reader interface {
	Read(ctx context.Context)
}

func New(brokerURL, topic string, chMap map[string]chan model.Price, redis service.RedisCasher) Reader {
	return &consumer{
		brokerURL: brokerURL,
		topic:     topic,
		chanels:   chMap,
		redis:     redis,
	}
}

func (c *consumer) Read(ctx context.Context) {
	log.Info("price-service Kafka reader started")
	for i := 0; i < 3; i++ {

		//log.Infof("input kafka_url from consumer.go: %v", c.brokerURL)

		go func(partition int) {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   []string{c.brokerURL},
				Topic:     c.topic,
				Partition: partition,
			})
			defer reader.Close()

			gorutineNum := reader.Config().Partition
			log.Info("Sucssesful start ", gorutineNum)

			for {
				msg, err := reader.ReadMessage(ctx)
				if err == io.EOF {
					log.Info(fmt.Sprintf("exiting kafka read gorutine-%d", gorutineNum))
					break
				}
				if err != nil {
					log.Error(fmt.Sprintf("error occurred in gorutine-%d: %v", gorutineNum, err))
					break
				}

				price := model.Price{}

				err = json.Unmarshal(msg.Value, &price)
				if err != nil {
					log.Errorf("Unmarshal error: %v", err)
					break
				}

				err = c.redis.Set(ctx, price)
				if err != nil {
					log.Errorf("Redis error: %v, exiting datastream", err)
					break //^?
				}

				log.Info("Kafka messafe read for ", price.Symbol)

				for _, ch := range c.chanels {
					log.Info("Sending for chanel: ", ch)
					ch <- price
					log.Info("Compleet sending for chanel: ", ch)
				}
			}
		}(i)
	}
	log.Info("price-service Kafka reader finished")
}
