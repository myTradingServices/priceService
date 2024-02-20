package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type consumer struct {
	brokerURL string
	topic     string
	chanels   map[string]chan model.Price
}

type Reader interface {
	Read(ctx context.Context)
}

func New(brokerURL, topic string, chMap map[string]chan model.Price) Reader {
	return &consumer{
		brokerURL: brokerURL,
		topic:     topic,
		chanels:   chMap,
	}
}

func (cons *consumer) Read(ctx context.Context) {
	for i := 0; i < 3; i++ {
		go func(partition int) {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   []string{cons.brokerURL},
				Topic:     cons.topic,
				Partition: partition,
			})

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

				for _, ch := range cons.chanels {
					ch <- price
				}
			}
		}(i)
	}
}
