package main

import (
	"context"
	"net"

	"github.com/mmfshirokan/PriceService/internal/config"
	"github.com/mmfshirokan/PriceService/internal/consumer"
	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/internal/repository"
	"github.com/mmfshirokan/PriceService/internal/rpc"
	"github.com/mmfshirokan/PriceService/proto/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	log.Info("Starting price service")

	ctx := context.Background()
	conf, err := config.New()
	if err != nil {
		log.Error(err)
		return
	}

	chanelsMap := map[string]chan model.Price{
		conf.RpcChartPort: make(chan model.Price), //uncooment
		conf.RpcPosPort:   make(chan model.Price), //coment if position service is not working!
		// ADD more chanelse if nessasry
	}

	client, err := repository.NewCLient(conf.RedisURL)
	if err != nil {
		log.Error(err)
		return
	}
	repo := repository.NewRedis(client)

	foreverChan := make(chan struct{})

	log.WithFields(log.Fields{
		"input kafka_url": conf.KafkaURL,
		"input redis_url": conf.RedisURL,
	}).Info("Input config from main.go:\n")

	cons := consumer.New(conf.KafkaURL, conf.KafkaTopic, chanelsMap, repo)
	go cons.Read(ctx)
	go rpcServerStart(chanelsMap, conf.RedisURL)

	log.Info("Price service working...")
	<-foreverChan
}

func rpcServerStart(chMap map[string]chan model.Price, redisURL string) {
	for port, chanel := range chMap {
		go func(prt string, ch chan model.Price) {
			lis, err := net.Listen("tcp", ":"+prt)
			if err != nil {
				log.Errorf("failed to listen: %v", err)
			}

			log.Infof("Listening on addres: %v", lis.Addr().String())

			client, err := repository.NewCLient(redisURL)
			if err != nil {
				log.Errorf("failed to connect to redis: %v", err)
			}
			repo := repository.NewRedis(client)

			rpcServer := grpc.NewServer()
			rpcConsumer := rpc.NewConsumerServer(ch, repo)

			pb.RegisterConsumerServer(rpcServer, rpcConsumer)

			err = rpcServer.Serve(lis)
			if err != nil {
				log.Error("rpc error: Server can't start")
			}
		}(port, chanel)
	}
}
