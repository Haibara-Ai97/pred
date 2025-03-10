package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

// RabbitMQConfig RabbitMQ 配置
type RabbitMQConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	VHost    string
}

// ModelRequest 模型服务请求
type ModelRequest struct {
	PointName    string `json:"point_name"`
	PreProModel  string `json:"pre_pro_model"`
	PredictModel string `json:"predict_model"`
	AssessModel  string `json:"assess_model"`
}

// ModelResponse 模型服务响应
type ModelResponse struct {
	PredIn   int    `json:"pred_in"`
	PredOut  int    `json:"pred_out"`
	Baseline int    `json:"baseline"`
	Error    string `json:"error,omitempty"`
}

type CalculateRequest struct {
	RequestID    string               `json:"request_id"`
	X            []float64            `json:"x"`
	Y            [][]float64          `json:"y"`
	X1           []float64            `json:"x1"`
	Y1           [][]float64          `json:"y1"`
	XNew         []float64            `json:"xNew"`
	PreProModel  string               `json:"prepro_model"`
	PredictModel string               `json:"predict_model"`
	AssessModel  string               `json:"assess_model"`
	PointName    string               `json:"point_name"`
	DataOrigin   map[string][]float64 `json:"data_origin"`
}

type CalculateResponse struct {
	RequestID     string      `json:"request_id"`
	DataOrigin    [][]float64 `json:"data_origin"`
	DataPrepare   [][]float64 `json:"data_prepare"`
	DataIntegrity float64     `json:"data_integrity"`
	AbnormPoints  []string    `json:"abnorm_points"`
	PreProModel   string      `json:"prepro_model"`
	PredictModel  string      `json:"predict_model"`
	AssessModel   string      `json:"assess_model"`
	IsModelChange int         `json:"model_change"`
	Error         string      `json:"error,omitempty"`
}

type PredictRequest struct {
	RequestID    string      `json:"request_id"`
	DataPrepare  [][]float64 `json:"data_prepare"`
	PreProModel  string      `json:"prepro_model"`
	PredictModel string      `json:"predict_model"`
	AssessModel  string      `json:"assess_model"`
	PointName    string      `json:"point_name"`
}

type PredictResponse struct {
	RequestID     string        `json:"request_id"`
	DataPredict   [][]float64   `json:"data_predict"`
	DataAssess    []interface{} `json:"data_assess"`
	PreProModel   string        `json:"prepro_model"`
	PredictModel  string        `json:"predict_model"`
	AssessModel   string        `json:"assess_model"`
	IsModelChange int           `json:"model_change"`
	Error         string        `json:"error,omitempty"`
}

type TotalRequest struct {
	RequestID    string               `json:"request_id"`
	X            []float64            `json:"x"`
	Y            [][]float64          `json:"y"`
	X1           []float64            `json:"x1"`
	Y1           [][]float64          `json:"y1"`
	XNew         []float64            `json:"xNew"`
	PreProModel  string               `json:"prepro_model"`
	PredictModel string               `json:"predict_model"`
	AssessModel  string               `json:"assess_model"`
	PointName    string               `json:"point_name"`
	DataOrigin   map[string][]float64 `json:"data_origin"`
}

type TotalResponse struct {
	RequestID     string        `json:"request_id"`
	DataOrigin    [][]float64   `json:"data_origin"`
	DataPrepare   [][]float64   `json:"data_prepare"`
	DataPredict   [][]float64   `json:"data_predict"`
	DataAssess    []interface{} `json:"data_assess"`
	DataIntegrity float64       `json:"data_integrity"`
	AbnormPoints  []string      `json:"abnorm_points"`
	PreProModel   string        `json:"prepro_model"`
	PredictModel  string        `json:"predict_model"`
	AssessModel   string        `json:"assess_model"`
	IsModelChange int           `json:"model_change"`
	Error         string        `json:"error,omitempty"`
}

type UpdateRequest struct {
	RequestID string `json:"request_id"`
	PointName string `json:"point_name"`
	PointType string `json:"point_type"`
}

type UpdateResponse struct {
	RequestID    string `json:"request_id"`
	PreProModel  string `json:"prepro_model"`
	PredictModel string `json:"predict_model"`
	AssessModel  string `json:"assess_model"`
	Error        string `json:"error,omitempty"`
}

// RabbitMQService RabbitMQ 服务
type RabbitMQService struct {
	config  RabbitMQConfig
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex
}

// NewRabbitMQService 创建 RabbitMQ 服务
func NewRabbitMQService(config RabbitMQConfig) (*RabbitMQService, error) {
	service := &RabbitMQService{
		config: config,
	}

	if err := service.connect(); err != nil {
		return nil, err
	}

	return service, nil
}

// connect 建立连接
func (s *RabbitMQService) connect() error {
	// 构建连接 URL
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		s.config.Username,
		s.config.Password,
		s.config.Host,
		s.config.Port,
		s.config.VHost)

	// 建立连接
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	s.conn = conn

	// 创建通道
	ch, err := conn.Channel()
	if err != nil {
		s.conn.Close()
		return fmt.Errorf("failed to open channel: %v", err)
	}
	s.channel = ch

	// 声明队列
	if err := s.declareQueues(); err != nil {
		s.Close()
		return err
	}

	return nil
}

func (s *RabbitMQService) createChannel() (*amqp.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Channel()
}

// declareQueues 声明必要的队列
func (s *RabbitMQService) declareQueues() error {
	// 声明请求队列
	_, err := s.channel.QueueDeclare(
		"model_requests", // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare request queue: %v", err)
	}

	// 声明响应队列
	_, err = s.channel.QueueDeclare(
		"model_responses", // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare response queue: %v", err)
	}

	// 声明数值计算请求队列
	_, err = s.channel.QueueDeclare(
		"calculate_requests", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare calculate request queue: %v", err)
	}

	// 声明数值计算响应队列
	_, err = s.channel.QueueDeclare(
		"calculate_responses", // name
		true,                  // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare calculate response queue: %v", err)
	}

	_, err = s.channel.QueueDeclare(
		"predict_requests", // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare predict request queue: %v", err)
	}

	_, err = s.channel.QueueDeclare(
		"predict_responses", // name
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare predict response queue: %v", err)
	}

	_, err = s.channel.QueueDeclare(
		"total_requests", // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare total request queue: %v", err)
	}

	_, err = s.channel.QueueDeclare(
		"total_responses", // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare total response queue: %v", err)
	}

	_, err = s.channel.QueueDeclare(
		"update_requests", // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare update request queue: %v", err)
	}

	_, err = s.channel.QueueDeclare(
		"total_responses", // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare update response queue: %v", err)
	}

	return nil
}

// Close 关闭连接
func (s *RabbitMQService) Close() error {
	if s.channel != nil {
		s.channel.Close()
	}
	if s.conn != nil {
		s.conn.Close()
	}
	return nil
}

// SendModelRequest 发送模型请求
func (s *RabbitMQService) SendModelRequest(ctx context.Context, req *ModelRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal SendModel Request: %v", err)
	}

	// 生成唯一的 correlationId
	correlationId := GenerateUUID()

	err = s.channel.Publish(
		"",               // exchange
		"model_requests", // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: correlationId,
			ReplyTo:       "model_responses",
			Body:          body,
		})
	if err != nil {
		return fmt.Errorf("failed to publish request: %v", err)
	}

	return nil
}

// ReceiveModelResponse 接收模型响应
func (s *RabbitMQService) ReceiveModelResponse(ctx context.Context) (*ModelResponse, error) {
	msgs, err := s.channel.Consume(
		"model_responses", // queue
		"",                // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %v", err)
	}

	select {
	case msg := <-msgs:
		var resp ModelResponse
		if err := json.Unmarshal(msg.Body, &resp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response: %v", err)
		}
		return &resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(1 * time.Second):
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

func (s *RabbitMQService) SendCalculateRequest(ctx context.Context, req *CalculateRequest) error {
	ch, err := s.createChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	body, err := json.Marshal(req)

	if err != nil {
		return fmt.Errorf("failed to marshal SendCalculate Request: %v", err)
	}

	//log.Printf("sending request to python calculate: %s", req.RequestID)
	err = ch.Publish(
		"",
		"calculate_requests",
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: req.RequestID,
			ReplyTo:       "calculate_responses",
			Body:          body,
		})
	if err != nil {
		return fmt.Errorf("failed to SendCalculate Request: %v", err)
	}

	return nil
}

func (s *RabbitMQService) ReceiveCalculateResponse(ctx context.Context, requestID string) (*CalculateResponse, error) {
	ch, err := s.createChannel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		"calculate_responses",
		fmt.Sprintf("consumer_%s", requestID),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %v", err)
	}

	// 创建一个单独的计时器
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	// 记录处理过的消息ID，避免重复处理
	processedMsgs := make(map[uint64]bool)

	for {
		select {
		case msg := <-msgs:
			// 检查是否已处理过这条消息
			if processedMsgs[msg.DeliveryTag] {
				continue
			}
			processedMsgs[msg.DeliveryTag] = true

			//log.Printf("Received message with correlation ID: %s (expecting: %s)", msg.CorrelationId, requestID)

			if msg.CorrelationId == requestID {
				var resp CalculateResponse
				if err := json.Unmarshal(msg.Body, &resp); err != nil {
					// 确认消息但返回错误
					ch.Ack(msg.DeliveryTag, false)
					return nil, fmt.Errorf("failed to unmarshal numerical response: %v", err)
				}
				// 确认消息并返回响应
				if err := ch.Ack(msg.DeliveryTag, false); err != nil {
					log.Printf("Error acknowledging message: %v", err)
				}
				//log.Printf("successfully received from python: %s", resp.RequestID)
				return &resp, nil
			} else {
				// 不匹配的消息直接确认，而不是放回队列
				// 因为其他消费者会处理自己的消息
				if err := ch.Nack(msg.DeliveryTag, false, true); err != nil {
					log.Printf("Error acknowledging unmatched message: %v", err)
				}
				// 重置超时计时器
				timer.Reset(2 * time.Second)
			}

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-timer.C:
			log.Printf("Timeout waiting for response to request: %s", requestID)
			return nil, fmt.Errorf("timeout waiting for numerical response for request: %s", requestID)
		}
	}
}

func (s *RabbitMQService) SendPredictRequest(ctx context.Context, req *PredictRequest) error {
	ch, err := s.createChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	body, err := json.Marshal(req)

	if err != nil {
		return fmt.Errorf("failed to marshal Send Predict Request: %v", err)
	}

	err = ch.Publish(
		"",
		"predict_requests",
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: req.RequestID,
			ReplyTo:       "predict_responses",
			Body:          body,
		})
	if err != nil {
		return fmt.Errorf("failed to Send Predict Request: %v", err)
	}

	return nil
}

func (s *RabbitMQService) ReceivePredictResponse(ctx context.Context, requestID string) (*PredictResponse, error) {
	ch, err := s.createChannel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		"predict_responses",
		fmt.Sprintf("consumer_%s", requestID),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %v", err)
	}

	// 创建一个单独的计时器
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	// 记录处理过的消息ID，避免重复处理
	processedMsgs := make(map[uint64]bool)

	for {
		select {
		case msg := <-msgs:
			// 检查是否已处理过这条消息
			if processedMsgs[msg.DeliveryTag] {
				continue
			}
			processedMsgs[msg.DeliveryTag] = true

			//log.Printf("Received message with correlation ID: %s (expecting: %s)", msg.CorrelationId, requestID)

			if msg.CorrelationId == requestID {
				var resp PredictResponse
				if err := json.Unmarshal(msg.Body, &resp); err != nil {
					// 确认消息但返回错误
					ch.Ack(msg.DeliveryTag, false)
					return nil, fmt.Errorf("failed to unmarshal numerical response: %v", err)
				}
				// 确认消息并返回响应
				if err := ch.Ack(msg.DeliveryTag, false); err != nil {
					log.Printf("Error acknowledging message: %v", err)
				}
				//log.Printf("successfully received from python: %s", resp.RequestID)
				return &resp, nil
			} else {
				// 不匹配的消息直接确认，而不是放回队列
				// 因为其他消费者会处理自己的消息
				if err := ch.Nack(msg.DeliveryTag, false, true); err != nil {
					log.Printf("Error acknowledging unmatched message: %v", err)
				}
				// 重置超时计时器
				timer.Reset(2 * time.Second)
			}

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-timer.C:
			log.Printf("Timeout waiting for response to request: %s", requestID)
			return nil, fmt.Errorf("timeout waiting for numerical response for request: %s", requestID)
		}
	}
}

func (s *RabbitMQService) SendTotalRequest(ctx context.Context, req *TotalRequest) error {
	ch, err := s.createChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	body, err := json.Marshal(req)

	if err != nil {
		return fmt.Errorf("failed to marshal SendCalculate Request: %v", err)
	}

	//log.Printf("sending request to python calculate: %s", req.RequestID)
	err = ch.Publish(
		"",
		"total_requests",
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: req.RequestID,
			ReplyTo:       "total_responses",
			Body:          body,
		})
	if err != nil {
		return fmt.Errorf("failed to SendCalculate Request: %v", err)
	}

	return nil
}

func (s *RabbitMQService) ReceiveTotalResponse(ctx context.Context, requestID string) (*TotalResponse, error) {
	ch, err := s.createChannel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		"total_responses",
		fmt.Sprintf("consumer_%s", requestID),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %v", err)
	}

	// 创建一个单独的计时器
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	// 记录处理过的消息ID，避免重复处理
	processedMsgs := make(map[uint64]bool)

	for {
		select {
		case msg := <-msgs:
			// 检查是否已处理过这条消息
			if processedMsgs[msg.DeliveryTag] {
				continue
			}
			processedMsgs[msg.DeliveryTag] = true

			//log.Printf("Received message with correlation ID: %s (expecting: %s)", msg.CorrelationId, requestID)

			if msg.CorrelationId == requestID {
				var resp TotalResponse
				if err := json.Unmarshal(msg.Body, &resp); err != nil {
					// 确认消息但返回错误
					ch.Ack(msg.DeliveryTag, false)
					return nil, fmt.Errorf("failed to unmarshal numerical response: %v", err)
				}
				// 确认消息并返回响应
				if err := ch.Ack(msg.DeliveryTag, false); err != nil {
					log.Printf("Error acknowledging message: %v", err)
				}
				//log.Printf("successfully received from python: %s", resp.RequestID)
				return &resp, nil
			} else {
				// 不匹配的消息直接确认，而不是放回队列
				// 因为其他消费者会处理自己的消息
				if err := ch.Nack(msg.DeliveryTag, false, true); err != nil {
					log.Printf("Error acknowledging unmatched message: %v", err)
				}
				// 重置超时计时器
				timer.Reset(5 * time.Second)
			}

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-timer.C:
			log.Printf("Timeout waiting for response to request: %s", requestID)
			return nil, fmt.Errorf("timeout waiting for numerical response for request: %s", requestID)
		}
	}
}

func (s *RabbitMQService) SendUpdateRequest(ctx context.Context, req *UpdateRequest) error {
	ch, err := s.createChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	body, err := json.Marshal(req)

	if err != nil {
		return fmt.Errorf("failed to marshal SendUpdate Request: %v", err)
	}

	//log.Printf("sending request to python calculate: %s", req.RequestID)
	err = ch.Publish(
		"",
		"update_requests",
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: req.RequestID,
			ReplyTo:       "update_responses",
			Body:          body,
		})
	if err != nil {
		return fmt.Errorf("failed to SendUpdate Request: %v", err)
	}

	return nil
}

func (s *RabbitMQService) ReceiveUpdateResponse(ctx context.Context, requestID string) (*UpdateResponse, error) {
	ch, err := s.createChannel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		"update_responses",
		fmt.Sprintf("consumer_%s", requestID),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %v", err)
	}

	// 创建一个单独的计时器
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	// 记录处理过的消息ID，避免重复处理
	processedMsgs := make(map[uint64]bool)

	for {
		select {
		case msg := <-msgs:
			// 检查是否已处理过这条消息
			if processedMsgs[msg.DeliveryTag] {
				continue
			}
			processedMsgs[msg.DeliveryTag] = true

			//log.Printf("Received message with correlation ID: %s (expecting: %s)", msg.CorrelationId, requestID)

			if msg.CorrelationId == requestID {
				var resp UpdateResponse
				if err := json.Unmarshal(msg.Body, &resp); err != nil {
					// 确认消息但返回错误
					ch.Ack(msg.DeliveryTag, false)
					return nil, fmt.Errorf("failed to unmarshal numerical response: %v", err)
				}
				// 确认消息并返回响应
				if err := ch.Ack(msg.DeliveryTag, false); err != nil {
					log.Printf("Error acknowledging message: %v", err)
				}
				//log.Printf("successfully received from python: %s", resp.RequestID)
				return &resp, nil
			} else {
				// 不匹配的消息直接确认，而不是放回队列
				// 因为其他消费者会处理自己的消息
				if err := ch.Nack(msg.DeliveryTag, false, true); err != nil {
					log.Printf("Error acknowledging unmatched message: %v", err)
				}
				// 重置超时计时器
				timer.Reset(2 * time.Second)
			}

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-timer.C:
			log.Printf("Timeout waiting for response to request: %s", requestID)
			return nil, fmt.Errorf("timeout waiting for numerical response for request: %s", requestID)
		}
	}
}

// generateUUID 生成唯一标识符
func GenerateUUID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
