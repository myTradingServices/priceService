package service

import (
	"context"

	"github.com/mmfshirokan/PriceService/internal/model"
	"github.com/mmfshirokan/PriceService/internal/repository"
)

type redisServ struct {
	repo repository.RedisInterface
}

type RedisCasher interface {
	Set(ctx context.Context, obj model.Price) error
	Get(ctx context.Context, key string) (model.Price, error)
}

func NewRedis(repo repository.RedisInterface) RedisCasher {
	return &redisServ{
		repo: repo,
	}
}

func (r *redisServ) Set(ctx context.Context, obj model.Price) error {
	return r.repo.Set(ctx, obj)
}

func (r *redisServ) Get(ctx context.Context, key string) (model.Price, error) {
	return r.repo.Get(ctx, key)
}
