package main

import (
	"context"
	"fmt"
	"time"

	"wb-L0/internal/kafka"
	"wb-L0/internal/models"

	"github.com/caarlos0/env/v6"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.InfoLevel)

	var cfg kafka.Config
	if err := env.Parse(&cfg); err != nil {
		logrus.WithError(err).Fatal("failed to load config")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5
	}

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		logrus.WithError(err).Fatal("failed to create producer")
	}
	defer producer.Close()

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		order := &models.Order{
			OrderUID:    fmt.Sprintf("order-%d", i),
			TrackNumber: fmt.Sprintf("TRACK%d", i),
			Entry:       "WBIL",
			Delivery:    models.Delivery{Name: "Test User", Phone: "+1234567890"},
			Payment:     models.Payment{Transaction: fmt.Sprintf("tx-%d", i), Amount: 100 + i},
			Items:       models.Items{{Name: fmt.Sprintf("Item %d", i), Price: 100 + i}},
			Locale:      "en",
			DateCreated: time.Now(),
		}
		if err := producer.SendMsg(ctx, order); err != nil {
			logrus.WithError(err).Errorf("failed to send message #%d", i)
		} else {
			logrus.Infof("Message #%d sent", i)
		}
		time.Sleep(1 * time.Second)
	}
	logrus.Info("All messages sent, producer exiting")
}
