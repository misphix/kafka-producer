package main

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

type Config struct {
	Address []string
	Topic   string
	Key     string
	Value   string
}

func main() {
	config, err := readConfig()
	if err != nil {
		log.Fatalln(err.Error())
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(config.Address, kafkaConfig)
	if err != nil {
		log.Fatalln(err.Error())
	}

	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: config.Topic,
		Key:   sarama.StringEncoder(config.Key),
		Value: sarama.StringEncoder(config.Value),
	}
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Printf("Partition: %d, offset: %d", partition, offset)
}

func readConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	config := &Config{}
	config.Address = viper.GetStringSlice("address")
	config.Topic = viper.GetString("topic")
	config.Key = viper.GetString("key")
	config.Value = viper.GetString("value")

	return config, nil
}
