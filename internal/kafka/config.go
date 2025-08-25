package kafka

type Config struct {
	Brokers []string `env:"KAFKA_BROKERS,required"`
	Topic   string   `env:"KAFKA_TOPIC,required"`
	GroupID string   `env:"KAFKA_GROUPID,required"`
	Timeout int      `env:"KAFKA_TIMEOUT"`
}
