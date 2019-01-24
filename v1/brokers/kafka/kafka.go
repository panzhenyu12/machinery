package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/panzhenyu12/machinery/v1/common"
	"github.com/panzhenyu12/machinery/v1/config"
)

//var redisDelayedTasksKey = "delayed_tasks"

// Broker represents a Redis broker
type Broker struct {
	common.Broker
	sub          *common.Subscriber
	pub          *common.Publisher
	processingWG sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
}

// New creates new Broker instance
func New(cnf *config.Config) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	sub, err := common.NewSubscriber(cnf.Kafka, nil, nil)
	if err != nil {
		panic("init kafka sub error")
	}
	pub, err := common.NewPublisher(cnf.Kafka)
	if err != nil {
		panic("init kafka sub error")
	}
	b.sub = sub
	b.pub = pub
	return b
}

func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	msgchan := b.sub.Start()
	log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

	if err := b.consume(msgchan, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	b.sub.Stop()
	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *Broker) Publish(signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	//log.INFO.Print("start to push ", string(msg))
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			delayMs := int64(signature.ETA.Sub(now) / time.Millisecond)

			return b.delay(signature, delayMs)
		}
	}
	err = b.pub.PublishRaw(nil, signature.RoutingKey, msg)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(maschan <-chan *common.SubMessage, concurrency int, taskProcessor iface.TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)

	for {
		err := b.sub.Err()
		if err != nil {
			return err
		}
		select {
		// case amqpErr <-b.sub.Err:
		// 	return amqpErr
		case err := <-errorsChan:
			return err
		case d := <-maschan:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}
			b.processingWG.Add(1)
			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.GetStopChan():
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery *common.SubMessage, taskProcessor iface.TaskProcessor) error {
	if len(delivery.Message()) == 0 {
		//b.pub.PublishRaw(nil,signature.Name,delivery.Message())
		return errors.New("Received an empty message") // RabbitMQ down?
	}

	//var multiple, requeue = false, false

	// Unmarshal message body into signature struct
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery.Message()))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		b.pub.PublishRaw(nil, signature.Name, delivery.Message())
		return errs.NewErrCouldNotUnmarshaTaskSignature(delivery.Message(), err)
	}

	// If the task is not registered, we nack it and requeue,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {

		log.INFO.Printf("Task not registered with this worker. Requeing message: %s", delivery.Message())
		b.pub.PublishRaw(nil, signature.Name, delivery.Message())
		//delivery.Nack(multiple, requeue)
		return nil
	}
	log.INFO.Printf("Received new message: %s", delivery.Message())
	err := taskProcessor.Process(signature)
	//delivery.Ack(multiple)
	return err
}

// delay a task by delayDuration miliseconds, the way it works is a new queue
// is created without any consumers, the message is then published to this queue
// with appropriate ttl expiration headers, after the expiration, it is sent to
// the proper queue with consumers
func (b *Broker) delay(signature *tasks.Signature, delayMs int64) error {
	if delayMs <= 0 {
		return errors.New("Cannot delay task by 0ms")
	}

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// It's necessary to redeclare the queue each time (to zero its TTL timer).
	b.pub.PublishRaw(nil, signature.RoutingKey, message)
	return nil
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	// if b.GetConfig().AMQP != nil && b.GetConfig().AMQP.ExchangeType == "direct" {
	// 	// The routing algorithm behind a direct exchange is simple - a message goes
	// 	// to the queues whose binding key exactly matches the routing key of the message.
	// 	s.RoutingKey = b.GetConfig().AMQP.BindingKey
	// 	return
	// }

	s.RoutingKey = b.GetConfig().DefaultQueue
}
