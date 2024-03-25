package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/internal/rpc/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	topic string = "prices"
)

var (
	chMap            map[string]chan model.Price
	kafkaHostAndPort string
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Errorf("Could not construct pool: %s", err)
		return
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Errorf("Could not connect to Docker: %s", err)
		return
	}

	resNet, err := pool.Client.PruneNetworks(docker.PruneNetworksOptions{})
	if err != nil {
		log.Error("prune network error: ", err)
	} else if len(resNet.NetworksDeleted) > 0 {
		log.Info("pruned networks: ", resNet.NetworksDeleted)
	} else {
		log.Info("no networks to prune")
	}

	resCont, err := pool.Client.PruneContainers(docker.PruneContainersOptions{})
	if err != nil {
		log.Error("prune containers error: ", err)
	} else if len(resCont.ContainersDeleted) > 0 {
		log.Info("pruned containers: ", resCont.ContainersDeleted)
	} else {
		log.Info("no containers to prune")
	}

	resImg, err := pool.Client.PruneImages(docker.PruneImagesOptions{})
	if err != nil {
		log.Error("prune images error: ", err)
	} else if len(resImg.ImagesDeleted) > 0 {
		log.Info("pruned images: ", resImg.ImagesDeleted)
	} else {
		log.Info("no images to prune")
	}

	resVol, err := pool.Client.PruneVolumes(docker.PruneVolumesOptions{})
	if err != nil {
		log.Error("prune volumes error: ", err)
	} else if len(resVol.VolumesDeleted) > 0 {
		log.Info("pruned volumes: ", resVol.VolumesDeleted)
	} else {
		log.Info("no volumes to prune")
	}

	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: "zookeeper_kafka_network"})
	if err != nil {
		log.Errorf("could not create a network to zookeeper and kafka: %s; Will try to prune networks", err)
		return
	}

	zookeeperResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         "zookeeper-test",
		Repository:   "wurstmeister/zookeeper",
		Tag:          "latest",
		NetworkID:    network.ID,
		Hostname:     "zookeeper",
		ExposedPorts: []string{"2181"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Errorf("Could not start zookeeper: %s", err)
		tmp, ok := pool.ContainerByName("zookeeper-test") // Why use tmp?
		if !ok {
			return
		}
		zookeeperResource = tmp
	}

	conn, _, err := zk.Connect([]string{fmt.Sprintf("127.0.0.1:%s", zookeeperResource.GetPort("2181/tcp"))}, 60*time.Second) // 10 Seconds
	if err != nil {
		log.Errorf("could not connect zookeeper: %s", err)
	}
	defer conn.Close()

	retryFn := func() error {
		switch conn.State() {
		case zk.StateHasSession, zk.StateConnected:
			return nil
		default:
			return errors.New("not yet connected")
		}
	}

	if err = pool.Retry(retryFn); err != nil {
		log.Errorf("could not connect to zookeeper: %s", err)
		return
	}

	kafkaResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "kafka-test",
		Repository: "wurstmeister/kafka",
		Tag:        "latest",
		NetworkID:  network.ID,
		Hostname:   "kafka",
		Env: []string{
			"KAFKA_CREATE_TOPICS=" + topic + ":1:1:compact",
			"KAFKA_ADVERTISED_LISTENERS=INSIDE://kafka:9092,OUTSIDE://localhost:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT",
			"KAFKA_LISTENERS=INSIDE://0.0.0.0:9092,OUTSIDE://0.0.0.0:9093",
			"KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
			"KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9093/tcp": {{HostIP: "localhost", HostPort: "9093/tcp"}},
		},
		ExposedPorts: []string{"9093/tcp"},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Errorf("could not start kafka: %s", err)
		tmp, ok := pool.ContainerByName("kafka-test") // Why use tmp?
		if !ok {
			return
		}
		kafkaResource = tmp
	}

	kafkaHostAndPort = kafkaResource.GetHostPort("9093/tcp")
	if kafkaHostAndPort == "" {
		log.Errorf("could not get kafka hostAndPort")
		return
	}

	chMap = map[string]chan model.Price{
		"9093": make(chan model.Price),
	}

	m.Run()

	if err = pool.Purge(zookeeperResource); err != nil {
		log.Errorf("could not purge zookeeperResource: %s", err)
		return
	}

	if err = pool.Purge(kafkaResource); err != nil {
		log.Errorf("could not purge kafkaResource: %s", err)
		return
	}

	if err = pool.Client.RemoveNetwork(network.ID); err != nil {
		log.Errorf("could not remove %s network: %s", network.Name, err)
		return
	}
}

func TestRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()

	date := time.Now()
	redisMock := mocks.NewRedisCasher(t)
	consumer := New(kafkaHostAndPort, topic, chMap, redisMock)

	testTable := []struct {
		name     string
		context  context.Context
		kafkaMsg []model.Price
	}{
		{
			name:    "Standart input with five msgs",
			context: ctx,
			kafkaMsg: []model.Price{
				{
					Date:   date,
					Bid:    decimal.New(1, 0),
					Ask:    decimal.New(2, 0),
					Symbol: "symb1",
				},
				{
					Date:   date,
					Bid:    decimal.New(3, 0),
					Ask:    decimal.New(4, 0),
					Symbol: "symb2",
				},
				{
					Date:   date,
					Bid:    decimal.New(5, 0),
					Ask:    decimal.New(6, 0),
					Symbol: "symb3",
				},
				{
					Date:   date,
					Bid:    decimal.New(7, 0),
					Ask:    decimal.New(8, 0),
					Symbol: "symb4",
				},
			},
		},
	}

	for _, test := range testTable {
		kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{kafkaHostAndPort},
			Topic:    topic,
			Async:    true,
			Balancer: &kafka.RoundRobin{},
		})

		go consumer.Read(test.context)

		for _, msg := range test.kafkaMsg {
			mockCall := redisMock.EXPECT().Set(mock.Anything, mock.Anything).Return(nil)
			jsonMarshaled, err := json.Marshal(msg)
			if err != nil {
				t.Fatalf("Unexpected marshal error: %v", err)
			}

			for {
				counter := 0

				err = kafkaWriter.WriteMessages(test.context, kafka.Message{
					Key:   []byte(msg.Symbol),
					Value: jsonMarshaled,
				})
				if err != nil {
					log.Errorf("Writing message error: %v", err)
					time.Sleep(time.Second * 3)
					if counter++; counter > 10 {
						log.Error("Too many attempts to write message, exiting writer with no msg sended")
						break
					}
					continue
				}

				break
			}

			// TODO add test for multiple connections
			actual := <-chMap["9093"]
			assert.Equal(t, msg.Ask, actual.Ask)
			assert.Equal(t, msg.Bid, actual.Bid)
			assert.Equal(t, msg.Symbol, actual.Symbol)

			if !msg.Date.Equal(actual.Date) {
				log.Error("Dates in passed msg are not equal")
			}
			redisMock.AssertExpectations(t)
			mockCall.Unset()
		}
	}
}

//
// mockCall := redisMock.EXPECT().Set(mock.Anything, model.Price{
// 	Date:   msg.Date,
// 	Bid:    msg.Bid,
// 	Ask:    msg.Ask,
// 	Symbol: msg.Symbol,
// }).Return(nil)
//
