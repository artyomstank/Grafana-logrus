package kafka

import (
	"context"
	"encoding/json"
	"time"
	"wb-l0/internal/models"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Brokers []string `env:"KAFKA_BROKERS,required"`
	Topic   string   `env:"KAFKA_TOPIC,required"`
	GroupID string   `env:"KAFKA_GROUPID,required"`
}

type Producer struct {
	writer *kafka.Writer
	topic  string
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
	return &Producer{writer: writer, topic: config.Topic}, nil
}

func (p *Producer) Close() error {
	if p.writer != nil {
		err := p.writer.Close()
		if err != nil {
			logrus.Errorf("ошибка при закрытии продюсера: %v", err)
			return err
		}
		logrus.Infof("продюсер для топика %s успешно закрыт", p.topic)
	}
	return nil
}

func (p *Producer) SendMsg(ctx context.Context, order models.Order) error {
	data_value, err := json.Marshal(order)
	if err != nil {
		logrus.Errorf("не удалось сериализовать заказ %s: %v", order.OrderUID, err)
		return err
	}
	msg := kafka.Message{
		Key:   []byte(order.OrderUID),
		Value: data_value,
		Time:  time.Now(),
	}
	SMcontext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.writer.WriteMessages(SMcontext, msg); err != nil {
		logrus.Errorf("не удалось отправить сообщение в продьюсере %s: %v", order.OrderUID, err)
		return err
	}
	logrus.Infof("сообщение %s было отправлено в кафку", order.OrderUID)
	return nil

}
