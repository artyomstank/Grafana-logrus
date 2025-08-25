package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"wb-L0/internal/kafka"

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

	consumer := kafka.NewConsumer(cfg)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		logrus.Warn("shutdown signal received, closing consumer...")
		cancel()
	}()

	consumer.ConsumeMessages(ctx)
}
