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
	"github.com/mmfshirokan/PriceService/internal/rpc/mocks"
	"github.com/mmfshirokan/PriceService/proto/pb"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	rpcConsumer := NewConsumerServer(ch, nil)

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
			log.Info("TestDataStream finished!")
			return
		}

	}
}

func TestGetLastPrice(t *testing.T) {
	localTarget := "localhost:3036"
	redisMock := mocks.NewRedisCasher(t)
	ctx, cansel := context.WithTimeout(context.Background(), time.Second*15)
	defer cansel()

	lis, err := net.Listen("tcp", localTarget)
	if err != nil {
		log.Errorf("failed to listen: %v", err)
	}

	rpcServer := grpc.NewServer()
	rpcConsumer := NewConsumerServer(nil, redisMock)

	pb.RegisterConsumerServer(rpcServer, rpcConsumer)
	go func() {
		err = rpcServer.Serve(lis)
		if err != nil {
			log.Error("rpc fatal error: Server can't start")
			return
		}
	}()

	option := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.Dial(localTarget, option)
	if err != nil {
		log.Error("can't connect to rpc on ", localTarget)
		return
	}
	defer conn.Close()
	client := pb.NewConsumerClient(conn)

	type T struct {
		name     string
		input    string
		expected model.Price
	}
	testTable := []T{
		{
			name:  "standart input-1",
			input: "symb1",
			expected: model.Price{
				Bid:    decimal.New(9, 0),
				Ask:    decimal.New(13, 0),
				Symbol: "symb1",
				Date:   time.Now(),
			},
		},
		{
			name:  "standart input-2",
			input: "symb2",
			expected: model.Price{
				Bid:    decimal.New(8, 0),
				Ask:    decimal.New(3, -1),
				Symbol: "symb2",
				Date:   time.Now(),
			},
		},
		{
			name:  "standart input-3",
			input: "symb3",
			expected: model.Price{
				Bid:    decimal.New(32, 0),
				Ask:    decimal.New(7, -1),
				Symbol: "symb3",
				Date:   time.Now(),
			},
		},
	}

	for _, test := range testTable {
		rdCall := redisMock.EXPECT().Get(mock.Anything, test.input).Return(test.expected, nil)
		actual, err := client.GetLastPrice(ctx, &pb.RequestGetLastPrice{Symbol: test.input})

		if ok := assert.Nil(t, err, test.name); !ok {
			continue
		}
		assert.Equal(t, test.expected.Ask, decimal.New(actual.Data.Ask.Value, actual.Data.Ask.Exp), test.name)
		assert.Equal(t, test.expected.Ask, decimal.New(actual.Data.Ask.Value, actual.Data.Ask.Exp), test.name)
		assert.Equal(t, test.expected.Symbol, actual.Data.Symbol, test.name)
		if ok := test.expected.Date.Equal(actual.Data.Date.AsTime()); !ok {
			t.Errorf("expected: %v, actual: %v", test.expected.Date, actual.Data.Date.AsTime())
		}

		redisMock.AssertExpectations(t)
		rdCall.Unset()
	}
	log.Info("GetLastPrice finished!")
}
