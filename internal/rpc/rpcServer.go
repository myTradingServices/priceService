package rpc

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/internal/service"
	"github.com/mmfshirokan/PriceService/proto/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type server struct {
	chanel chan model.Price
	serv   service.RedisCasher
	pb.UnimplementedConsumerServer
}

func NewConsumerServer(ch chan model.Price, serv service.RedisCasher) pb.ConsumerServer {
	return &server{
		chanel: ch,
		serv:   serv,
	}
}

func (s *server) DataStream(req *pb.RequestDataStream, stream pb.Consumer_DataStreamServer) error {
	log.Info("Data stream started")

	if !req.Start {
		log.Info("Data stream exited (request start is not true)")
		return errors.New("start is not initiated")
	}

	for {
		log.Info("Recive from ch: ", s.chanel)
		price := <-s.chanel
		log.Info("Recive completed for ch: ", s.chanel)

		err := stream.Send(&pb.ResponseDataStream{
			Date: timestamppb.New(price.Date),
			Bid: &pb.ResponseDataStreamDecimal{
				Value: price.Bid.CoefficientInt64(),
				Exp:   price.Bid.Exponent(),
			},
			Ask: &pb.ResponseDataStreamDecimal{
				Value: price.Ask.CoefficientInt64(),
				Exp:   price.Ask.Exponent(),
			},
			Symbol: price.Symbol,
		})
		if err == io.EOF {
			log.Infof("Stream exited, because error is: %v", err)
			break
		}
		if err != nil {
			log.Errorf("Error sending message: %v.", err)
		}

		log.Infof("Message sent for symbol: %v, on: %v", price.Symbol, time.Now().String())

		//time.Sleep(time.Second)//?
	}

	log.Info("Data stream exited")

	return nil
}

func (s *server) GetLastPrice(ctx context.Context, req *pb.RequestGetLastPrice) (resp *pb.ResponseGetLastPrice, err error) {
	log.Info("GetLastPrice called")

	price, err := s.serv.Get(ctx, req.Symbol)
	if err != nil {
		log.Error("GetLastPrice error: ", err)
		return nil, err
	}

	log.Info("GetLastPrice call completed")

	return &pb.ResponseGetLastPrice{
		Data: &pb.ResponseDataStream{
			Date: timestamppb.New(price.Date),
			Bid: &pb.ResponseDataStreamDecimal{
				Value: price.Bid.CoefficientInt64(),
				Exp:   price.Bid.Exponent(),
			},
			Ask: &pb.ResponseDataStreamDecimal{
				Value: price.Ask.CoefficientInt64(),
				Exp:   price.Ask.Exponent(),
			},
			Symbol: price.Symbol,
		},
	}, nil
}
