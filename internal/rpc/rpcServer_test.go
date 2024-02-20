package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/proto/pb"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	target      string = "localhost:2021"
	msgQuantity int    = 10
)

func TestMain(m *testing.M) {
	ch := make(chan model.Price)

	lis, err := net.Listen("tcp", target)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}

	rpcServer := grpc.NewServer()
	rpcConsumer := NewConsumerServer(ch)

	pb.RegisterConsumerServer(rpcServer, rpcConsumer)

	go func(chanel chan model.Price) {
		for i := 0; i < msgQuantity; i++ {
			ch <- model.Price{
				Bid:    decimal.New(int64(i), 0),
				Ask:    decimal.New(int64(i), 0),
				Symbol: "symb" + fmt.Sprint(i),
			}
		}
	}(ch)

	go func() {
		err = rpcServer.Serve(lis)
		if err != nil {
			log.Error("rpc fatal error: Server can't start")
			return
		}
	}()

	code := m.Run()

	os.Exit(code)
}

func TestDataStream(t *testing.T) {
	ctx, cansel := context.WithTimeout(context.Background(), time.Second*15)
	defer cansel()

	option := grpc.WithTransportCredentials(insecure.NewCredentials())

	conn, err := grpc.Dial(target, option)
	if err != nil {
		log.Error("can't connect to rpc on ", target)
		return
	}
	defer conn.Close()

	client := pb.NewConsumerClient(conn)

	stream, err := client.DataStream(ctx, &pb.RequestDataStream{Start: true})
	if err != nil {
		log.Errorf("data stream error: %v", err)
	}

	counter := 0
	for {
		recv, err := stream.Recv()
		if err == io.EOF {
			log.Infof("Exitin stream, because error is %v", err)
			break
		}
		if err != nil {
			log.Errorf("Error occured: %v", err)
			return
		}

		assert.Equal(
			t,
			decimal.New(int64(counter), 0),
			decimal.New(recv.Bid.Value, recv.Bid.Exp),
		)
		assert.Equal(
			t,
			decimal.New(int64(counter), 0),
			decimal.New(recv.Ask.Value, recv.Ask.Exp),
		)
		assert.Equal(
			t,
			"symb"+fmt.Sprint(counter),
			recv.Symbol,
		)

		counter++
		if counter == msgQuantity {
			stream.CloseSend()
			return
		}

	}
}
