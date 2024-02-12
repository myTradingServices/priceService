package model

import (
	"time"

	"github.com/shopspring/decimal"
)

type Price struct {
	Date   time.Time
	Bid    decimal.Decimal
	Ask    decimal.Decimal
	Symbol string
}
