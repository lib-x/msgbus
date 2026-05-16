package msgbus

import (
	"context"
	"errors"
	"io"

	msgbusv1 "github.com/example/msgbus/go-sdk/msgbus/gen/msgbus/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeID = msgbusv1.NodeId
type Message = msgbusv1.MessageEnvelope
type FifoMessage = msgbusv1.FifoMessage
type TopicHead = msgbusv1.TopicHead

type Client struct {
	conn          *grpc.ClientConn
	rpc           msgbusv1.MsgbusServiceClient
	defaultOrigin *msgbusv1.NodeId
}

func Dial(ctx context.Context, target string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	client := &Client{
		conn: conn,
		rpc:  msgbusv1.NewMsgbusServiceClient(conn),
	}
	conn.Connect()
	return client, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) WithDefaultOrigin(origin *msgbusv1.NodeId) *Client {
	c.defaultOrigin = origin
	return c
}

func (c *Client) Health(ctx context.Context) (string, error) {
	resp, err := c.rpc.Health(ctx, &msgbusv1.HealthRequest{})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

func (c *Client) Publish(ctx context.Context, topic string, payload []byte) (*msgbusv1.MessageEnvelope, error) {
	return c.PublishWithHeaders(ctx, topic, payload, nil)
}

func (c *Client) PublishWithHeaders(ctx context.Context, topic string, payload []byte, headers map[string]string) (*msgbusv1.MessageEnvelope, error) {
	resp, err := c.rpc.Publish(ctx, &msgbusv1.PublishRequest{
		Topic:   topic,
		Origin:  c.defaultOrigin,
		Payload: payload,
		Headers: headers,
	})
	if err != nil {
		return nil, err
	}
	if resp.Message == nil {
		return nil, errors.New("msgbus: empty publish response")
	}
	return resp.Message, nil
}

func (c *Client) Fetch(ctx context.Context, topic string, origin *msgbusv1.NodeId, fromHeadID uint64, limit uint32) ([]*msgbusv1.MessageEnvelope, error) {
	if origin == nil {
		origin = c.defaultOrigin
	}
	stream, err := c.rpc.Fetch(ctx, &msgbusv1.FetchRequest{
		Topic:      topic,
		Origin:     origin,
		FromHeadId: fromHeadID,
		Limit:      limit,
	})
	if err != nil {
		return nil, err
	}
	var messages []*msgbusv1.MessageEnvelope
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return messages, nil
		}
		if err != nil {
			return nil, err
		}
		if resp.Message != nil {
			messages = append(messages, resp.Message)
		}
	}
}

func (c *Client) Subscribe(ctx context.Context, topic string, origin *msgbusv1.NodeId, fromHeadID uint64) (msgbusv1.MsgbusService_SubscribeClient, error) {
	if origin == nil {
		origin = c.defaultOrigin
	}
	return c.rpc.Subscribe(ctx, &msgbusv1.SubscribeRequest{
		Topic:       topic,
		Origin:      origin,
		FromHeadId:  fromHeadID,
		ReplayLimit: 100,
	})
}

func (c *Client) GetHead(ctx context.Context, topic string, origin *msgbusv1.NodeId) (*msgbusv1.GetHeadResponse, error) {
	if origin == nil {
		origin = c.defaultOrigin
	}
	return c.rpc.GetHead(ctx, &msgbusv1.GetHeadRequest{
		Topic:  topic,
		Origin: origin,
	})
}

func (c *Client) ListHeads(ctx context.Context) ([]*msgbusv1.TopicHead, error) {
	resp, err := c.rpc.ListHeads(ctx, &msgbusv1.ListHeadsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Heads, nil
}

func (c *Client) DeleteRange(ctx context.Context, topic string, origin *msgbusv1.NodeId, fromHeadID uint64, toHeadID uint64) (uint64, error) {
	if origin == nil {
		origin = c.defaultOrigin
	}
	resp, err := c.rpc.DeleteRange(ctx, &msgbusv1.DeleteRangeRequest{
		Topic:      topic,
		Origin:     origin,
		FromHeadId: fromHeadID,
		ToHeadId:   toHeadID,
	})
	if err != nil {
		return 0, err
	}
	return resp.DeletedCount, nil
}

func (c *Client) EnqueueFIFO(ctx context.Context, queue string, topic string, target *msgbusv1.NodeId, payload []byte) (*msgbusv1.FifoMessage, error) {
	resp, err := c.rpc.EnqueueFifo(ctx, &msgbusv1.EnqueueFifoRequest{
		Queue:   queue,
		Topic:   topic,
		Source:  c.defaultOrigin,
		Target:  target,
		Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	if resp.Message == nil {
		return nil, errors.New("msgbus: empty fifo response")
	}
	return resp.Message, nil
}

func (c *Client) PeekFIFO(ctx context.Context, queue string) (*msgbusv1.FifoMessage, error) {
	resp, err := c.rpc.PeekFifo(ctx, &msgbusv1.PeekFifoRequest{Queue: queue})
	if err != nil {
		return nil, err
	}
	if resp.Message == nil {
		return nil, errors.New("msgbus: empty fifo response")
	}
	return resp.Message, nil
}

func (c *Client) AckFIFO(ctx context.Context, queue string, messageID string) (bool, error) {
	resp, err := c.rpc.AckFifo(ctx, &msgbusv1.AckFifoRequest{
		Queue:     queue,
		MessageId: messageID,
	})
	if err != nil {
		return false, err
	}
	return resp.Accepted, nil
}

func (c *Client) RejectFIFO(ctx context.Context, queue string, messageID string) (bool, error) {
	resp, err := c.rpc.RejectFifo(ctx, &msgbusv1.RejectFifoRequest{
		Queue:     queue,
		MessageId: messageID,
	})
	if err != nil {
		return false, err
	}
	return resp.Accepted, nil
}
