package rpc

import (
	"errors"
	"io"
	"time"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/proto/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	chanel chan model.Price
	pb.UnimplementedConsumerServer
}

func NewConsumerServer(ch chan model.Price) pb.ConsumerServer {
	return &Server{
		chanel: ch,
	}
}

func (serv *Server) DataStream(req *pb.RequestDataStream, stream pb.Consumer_DataStreamServer) error {
	if !req.Start {
		return errors.New("start is not initiated")
	}

	for {
		price := <-serv.chanel
		err := stream.Send(&pb.ResponseDataStream{
			Date: timestamppb.New(price.Date),
			Bid: &pb.ResponseDataStreamDecimal{
				Value: price.Bid.BigInt().Int64(),
				Exp:   price.Bid.Exponent(),
			},
			Ask: &pb.ResponseDataStreamDecimal{
				Value: price.Ask.BigInt().Int64(),
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

		time.Sleep(time.Second)
	}

	return nil
}
