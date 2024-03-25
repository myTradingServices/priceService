package repository

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	connRedis RedisInterface

	stdInput1 = model.Price{
		Date:   time.Now(),
		Bid:    decimal.New(13, 0),
		Ask:    decimal.New(123, -1),
		Symbol: "symb1",
	}

	stdInput2 = model.Price{
		Date:   time.Now(),
		Bid:    decimal.New(13000, 0),
		Ask:    decimal.New(123, -2),
		Symbol: "symb2",
	}

	stdInput3 = model.Price{
		Date:   time.Now(),
		Bid:    decimal.New(131313, 0),
		Ask:    decimal.New(12331, -3),
		Symbol: "symb3",
	}

	repeatInput1 = model.Price{
		Date:   time.Now(),
		Bid:    decimal.New(1300, 0),
		Ask:    decimal.New(1222, -2),
		Symbol: "symb1",
	}

	repeatInput2 = model.Price{
		Date:   time.Now(),
		Bid:    decimal.New(1233, -2),
		Ask:    decimal.New(1230, 0),
		Symbol: "symb2",
	}

	repeatInput3 = model.Price{
		Date:   time.Now(),
		Bid:    decimal.New(313, 0),
		Ask:    decimal.New(11, -3),
		Symbol: "symb3",
	}
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Error("Could not construct pool: ", err)
		return
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Error("Could not connect to Docker: ", err)
		return
	}

	rdResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Hostname:   "redis_test",
		Repository: "redis",
		Tag:        "latest",
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Error("Could not start resource: ", err)
		return
	}

	redisHostAndPort := rdResource.GetHostPort("6379/tcp")
	redisUrl := fmt.Sprintf("redis://%s/0?protocol=3", redisHostAndPort)

	log.Println("Connecting to redis on url: ", redisUrl)

	var client *redis.Client
	if err = pool.Retry(func() error {
		client, err = NewCLient(redisUrl)
		if err != nil {
			return err
		}
		return client.Ping(ctx).Err()
	}); err != nil {
		log.Error("Could not connect to docker: ", err)
		return
	}

	connRedis = NewRedis(client)

	code := m.Run()

	if err := pool.Purge(rdResource); err != nil {
		log.Error("Could not purge resource: ", err)
		return
	}

	os.Exit(code)
}

func TestSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	type T struct {
		name string
		obj  model.Price
	}
	testTable := []T{
		{
			name: "standart input-1",
			obj:  stdInput1,
		},
		{
			name: "standart input-2",
			obj:  stdInput2,
		},
		{
			name: "standart input-3",
			obj:  stdInput3,
		},
	}

	var wg sync.WaitGroup
	for _, testCase := range testTable {
		wg.Add(1)
		go func(ctxx context.Context, test T) {
			defer wg.Done()
			err := connRedis.Set(ctx, test.obj)
			assert.Nil(t, err, test.name)
		}(ctx, testCase)
	}
	wg.Wait()

	testTable = []T{
		{
			name: "repeated input-1",
			obj:  repeatInput1,
		},
		{
			name: "repeated input-2",
			obj:  repeatInput2,
		},
		{
			name: "repeated input-3",
			obj:  repeatInput3,
		},
	}

	for _, testCase := range testTable {
		wg.Add(1)
		go func(ctxx context.Context, test T) {
			defer wg.Done()
			err := connRedis.Set(ctx, test.obj)
			assert.Nil(t, err, test.name)
		}(ctx, testCase)
	}
	wg.Wait()

	log.Info("TestSet finished!")
}

func TestGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	type T struct {
		name     string
		key      string
		expected model.Price
		Oldest   bool
	}
	testTable := []T{
		{
			name:     "standart input-1 with error",
			key:      "symb1",
			expected: stdInput1,
			Oldest:   true,
		},
		{
			name:     "standart input-2 with error",
			key:      "symb2",
			expected: stdInput2,
			Oldest:   true,
		},
		{
			name:     "standart input-3 with error",
			key:      "symb3",
			expected: stdInput3,
			Oldest:   true,
		},
		{
			name:     "standart input-1",
			key:      "symb1",
			expected: repeatInput1,
			Oldest:   false,
		},
		{
			name:     "standart input-2",
			key:      "symb2",
			expected: repeatInput2,
			Oldest:   false,
		},
		{
			name:     "standart input-3",
			key:      "symb3",
			expected: repeatInput3,
			Oldest:   false,
		},
	}

	var wg sync.WaitGroup
	for _, testCase := range testTable {
		wg.Add(1)
		go func(ctxx context.Context, test T) {
			defer wg.Done()
			actual, err := connRedis.Get(ctx, test.key)
			if test.Oldest {
				if ok := assert.Nil(t, err, test.name); !ok {
					return
				}
				assert.NotEqual(t, test.expected.Ask, actual.Ask, test.name)
				assert.NotEqual(t, test.expected.Bid, actual.Bid, test.name)

				if ok := test.expected.Date.Equal(actual.Date); ok {
					t.Error("Dates in passed old msg are equal")
				}
			} else {
				if ok := assert.Nil(t, err, test.name); !ok {
					return
				}
				assert.Equal(t, test.expected.Ask, actual.Ask, test.name)
				assert.Equal(t, test.expected.Bid, actual.Bid, test.name)
				assert.Equal(t, test.expected.Symbol, actual.Symbol, test.name)

				if ok := test.expected.Date.Equal(actual.Date); !ok {
					t.Error("Dates in passed msg are not equal")
				}
			}
		}(ctx, testCase)

	}

	wg.Wait()
	log.Info("TestGet finished!")
}
