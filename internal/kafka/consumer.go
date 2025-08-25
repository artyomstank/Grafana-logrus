package kafka

import (
	"context"
	"encoding/json"
	"time"
	"wb-L0/internal/models"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	reader *kafka.Reader
	topic  string
	group  string
}

// NewConsumer создаёт Kafka consumer
func NewConsumer(config Config) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		GroupID:        config.GroupID,
		Topic:          config.Topic,
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		CommitInterval: time.Second, // авто-коммит кажду секунду
	})
	logrus.WithFields(logrus.Fields{
		"brokers": config.Brokers,
		"topic":   config.Topic,
		"group":   config.GroupID,
	}).Info("Kafka consumer initialized")

	return &Consumer{reader: reader, topic: config.Topic, group: config.GroupID}
}

// Close закрывает consumer
func (c *Consumer) Close() error {
	if c.reader != nil {
		err := c.reader.Close()
		c.reader = nil
		if err != nil {
			logrus.Errorf("ошибка при закрытии consumer: %v", err)
			return err
		}
		logrus.Infof("consumer для топика %s успешно закрыт", c.topic)
	}
	return nil
}

// ConsumeMessages слушает Kafka и обрабатывает сообщения
func (c *Consumer) ConsumeMessages(ctx context.Context) {
	logrus.Infof("Старт чтения сообщений из Kafka (topic: %s, group: %s)", c.topic, c.group)
	for {
		if ctx.Err() != nil {
			logrus.Info("consumer stopped by context")
			return
		}
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logrus.Info("consumer stopped by context")
				return
			}
			logrus.WithError(err).Error("read message error")
			time.Sleep(time.Second)
			continue
		}
		var order models.Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			logrus.WithError(err).Error("unmarshal order error")
			continue
		}
		logrus.WithFields(logrus.Fields{
			"partition": m.Partition,
			"offset":    m.Offset,
			"order_uid": order.OrderUID,
		}).Info("message received from kafka")
	}
}
