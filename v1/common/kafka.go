package common

import (
	"errors"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/log"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"github.com/panzhenyu12/machinery/v1/config"
	"golang.org/x/net/context"
)

var (
	RequiredAcks = sarama.WaitForAll
)

type Publisher struct {
	producer sarama.SyncProducer
	topic    string
}

func NewPublisher(cfg *config.KafkaConfig) (*Publisher, error) {
	var err error
	p := &Publisher{}

	if len(cfg.Topic) == 0 {
		return p, errors.New("topic name is required")
	}
	p.topic = cfg.Topic
	cfg.BrokerHosts = strings.Split(cfg.BrokerHostsString, ",")
	sconfig := sarama.NewConfig()
	sconfig.Producer.Retry.Max = cfg.MaxRetry
	sconfig.Producer.RequiredAcks = RequiredAcks

	sconfig.Producer.Return.Successes = true
	p.producer, err = sarama.NewSyncProducer(cfg.BrokerHosts, sconfig)
	return p, err
}

func (p *Publisher) Publish(ctx context.Context, key string, m proto.Message) error {
	mb, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return p.PublishRaw(ctx, key, mb)
}

func (p *Publisher) PublishRaw(_ context.Context, key string, m []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(m),
	}
	_, _, err := p.producer.SendMessage(msg)
	return err
}

func (p *Publisher) Stop() error {
	return p.producer.Close()
}

type (
	Subscriber struct {
		cnsmr     *cluster.Consumer
		topic     string
		partition int32

		offset          func() int64
		broadcastOffset func(int64)

		kerr error

		stop chan chan error
	}

	SubMessage struct {
		message         *sarama.ConsumerMessage
		broadcastOffset func(int64)
	}
)

func (m *SubMessage) Message() []byte {
	return m.message.Value
}

func (m *SubMessage) ExtendDoneDeadline(time.Duration) error {
	return nil
}

func (m *SubMessage) Done() error {
	m.broadcastOffset(m.message.Offset)
	return nil
}

func NewSubscriber(cfg *config.KafkaConfig, offsetProvider func() int64, offsetBroadcast func(int64)) (*Subscriber, error) {
	var (
		err error
	)
	s := &Subscriber{
		offset:          offsetProvider,
		broadcastOffset: offsetBroadcast,
		partition:       cfg.Partition,
		stop:            make(chan chan error, 1),
	}
	cfg.BrokerHosts = strings.Split(cfg.BrokerHostsString, ",")
	if len(cfg.BrokerHosts) == 0 {
		return s, errors.New("at least 1 broker host is required")
	}

	if len(cfg.Topic) == 0 {
		return s, errors.New("topic name is required")
	}
	s.topic = cfg.Topic

	sconfig := cluster.NewConfig()
	sconfig.Consumer.Return.Errors = true
	sconfig.Group.Return.Notifications = true
	s.cnsmr, err = cluster.NewConsumer(cfg.BrokerHosts, cfg.KafkaGroupID, []string{cfg.Topic}, sconfig)
	return s, err
}

func (s *Subscriber) Start() <-chan *SubMessage {
	output := make(chan *SubMessage, 1000)
	go func() {
		for {
			select {
			case exit := <-s.stop:
				exit <- s.cnsmr.Close()
				return
			case kerr := <-s.cnsmr.Errors():
				s.kerr = kerr
				break
			case info := <-s.cnsmr.Notifications():
				log.INFO.Print("Kafka notification:", info)
				break
			case msg, _ := <-s.cnsmr.Messages():
				//log.INFO.Print("get msg", string(msg.Value))
				output <- &SubMessage{
					message:         msg,
					broadcastOffset: s.broadcastOffset,
				}
			}
		}
	}()
	return output
}

func (s *Subscriber) Stop() error {
	exit := make(chan error)
	s.stop <- exit
	// close result from the partition consumer
	err := <-exit
	if err != nil {
		return err
	}
	return s.cnsmr.Close()
}

func (s *Subscriber) Err() error {
	return s.kerr
}

func GetPartitions(brokerHosts []string, topic string) (partitions []int32, err error) {
	if len(brokerHosts) == 0 {
		return partitions, errors.New("at least 1 broker host is required")
	}

	if len(topic) == 0 {
		return partitions, errors.New("topic name is required")
	}

	var cnsmr sarama.Consumer
	cnsmr, err = sarama.NewConsumer(brokerHosts, sarama.NewConfig())
	if err != nil {
		return partitions, err
	}

	defer func() {
		if cerr := cnsmr.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	return cnsmr.Partitions(topic)
}
