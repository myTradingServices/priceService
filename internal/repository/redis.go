package repository

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/redis/go-redis/v9"
)

type redisRepo struct {
	client *redis.Client
	mut    sync.RWMutex
}

type RedisInterface interface {
	Set(ctx context.Context, obj model.Price) error
	Get(ctx context.Context, key string) (model.Price, error)
}

func NewRedis(client *redis.Client) RedisInterface {
	return &redisRepo{
		client: client,
	}
}

func (r *redisRepo) Set(ctx context.Context, obj model.Price) error {
	marshObj, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	r.mut.Lock()
	err = r.client.Set(ctx, obj.Symbol, marshObj, time.Second*2).Err()
	r.mut.Unlock()

	return err
}

func (r *redisRepo) Get(ctx context.Context, key string) (obj model.Price, err error) {
	var marshObj []byte

	r.mut.RLock()
	err = r.client.Get(ctx, key).Scan(&marshObj)
	r.mut.RUnlock()

	if err != nil {
		return model.Price{}, err
	}

	err = json.Unmarshal(marshObj, &obj)
	if err != nil {
		return model.Price{}, err
	}

	return obj, nil
}

func NewCLient(redisURI string) (client *redis.Client, err error) {
	opt, err := redis.ParseURL(redisURI)
	client = redis.NewClient(opt)
	return
}
