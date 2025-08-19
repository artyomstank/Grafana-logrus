package main

import (
	"context"
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
		logrus.WithError(err).Fatal("не удалось загрузить конфиг")
	}

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		logrus.WithError(err).Fatal("не удалось создать Kafka producer")
	}
	defer producer.Close()

	// Тестовые данные
	order := models.Order{
		OrderUID:    "test-123",
		TrackNumber: "TRACK-001",
		Locale:      "en",
		DateCreated: time.Now().UTC(),
	}

	// Отправляем сообщение с таймаутом
	ctx := context.Background()
	if err := producer.SendMsg(ctx, order); err != nil {
		logrus.WithError(err).Error("ошибка при отправке сообщения")
	} else {
		logrus.WithField("order_uid", order.OrderUID).Info("сообщение успешно отправлено")
	}
	// producer, err := kafka.NewProducer(cfg)
	// if err != nil {
	// 	logrus.WithError(err).Fatal("не удалось создать Kafka producer")
	// }
	// defer producer.Close()

	// ctx := context.Background()
	// time.Sleep(10 * time.Second)
	// for i := 1; i <= 5; i++ { // отправляем 5 сообщений
	// 	order := models.Order{
	// 		OrderUID:    fmt.Sprintf("test-%d", i),
	// 		TrackNumber: fmt.Sprintf("TRACK-%03d", i),
	// 		Locale:      "en",
	// 		DateCreated: time.Now().UTC(),
	// 	}

	// 	if err := producer.SendMsg(ctx, order); err != nil {
	// 		logrus.WithError(err).Error("ошибка при отправке сообщения")
	// 	} else {
	// 		logrus.WithField("order_uid", order.OrderUID).Info("сообщение успешно отправлено")
	// 	}

	// 	time.Sleep(2 * time.Second) // пауза между сообщениями
	// }

	// logrus.Info("Все сообщения отправлены")
}
