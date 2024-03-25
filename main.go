package main

import (
	"context"
	"net"
	"strings"

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
	ctx := context.Background()
	conf, err := config.New()
	if err != nil {
		log.Error(err)
		return
	}

	chanelsMap := map[string]chan model.Price{
		getPort(conf.RpcChartPort): make(chan model.Price),
		getPort(conf.RpcPosPort):   make(chan model.Price),
		// ADD more chanelse if nessasry
	}

	client, err := repository.NewCLient(conf.RedisURL)
	if err != nil {
		log.Error(err)
		return
	}
	repo := repository.NewRedis(client)

	foreverChan := make(chan struct{})

	cons := consumer.New(conf.KafkaURL, conf.KafkaTopic, chanelsMap, repo)
	go cons.Read(ctx)
	go rpcServerStart(chanelsMap, conf.RedisURL)

	<-foreverChan
}

func rpcServerStart(chMap map[string]chan model.Price, redisURL string) {

	for port, chanel := range chMap {
		go func(prt string, ch chan model.Price) {
			lis, err := net.Listen("tcp", "localhost:"+prt)
			if err != nil {
				log.Errorf("failed to listen: %v", err)
			}

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

func getPort(url string) string {
	_, port, found := strings.Cut(url, ":")
	if !found {
		log.Error("Empty port-env, using default env for map-key 7070")
		return "7070"
	}

	return port
}
