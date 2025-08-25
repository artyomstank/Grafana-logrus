package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"wb-L0/internal/models"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Producer struct {
	writer  *kafka.Writer
	topic   string
	timeout time.Duration
}

func NewProducer(config Config) (*Producer, error) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  config.Brokers,
		Topic:    config.Topic,
		Balancer: &kafka.Hash{},
		Async:    false,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logrus.Errorf(msg, args...)
		}),
		RequiredAcks: int(kafka.RequireAll),
	})
	to := 5 * time.Second
	if config.Timeout > 0 {
		to = time.Duration(config.Timeout) * time.Second
	}
	return &Producer{writer: writer, topic: config.Topic, timeout: to}, nil
}

func (p *Producer) Close() error {
	if p.writer != nil {
		err := p.writer.Close()
		p.writer = nil
		return err
	}
	return nil
}

func (p *Producer) SendMsg(ctx context.Context, order *models.Order) error {
	if p.writer == nil {
		return fmt.Errorf("writer is nil")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	value, err := json.Marshal(order)
	if err != nil {
		logrus.WithError(err).Error("marshal order error")
		return err
	}
	msg := kafka.Message{
		Key:   []byte(order.OrderUID),
		Value: value,
		Time:  time.Now(),
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if err := p.writer.WriteMessages(ctxTimeout, msg); err != nil {
		logrus.WithError(err).Error("send message error")
		return err
	}
	logrus.WithField("order_uid", order.OrderUID).Info("message sent to kafka")
	return nil
}
