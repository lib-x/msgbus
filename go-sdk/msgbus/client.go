// Package msgbus provides a Go SDK for applications that talk to a local
// msgbusd daemon over gRPC.
//
// The SDK is intentionally thin. It handles connection setup, default origins,
// retries for subscription streams, and convenience wrappers around the
// protobuf API. Durable storage, ordering, and peer synchronization are handled
// by msgbusd.
package msgbus

import (
	"context"
	"errors"
	"io"
	"time"

	msgbusv1 "github.com/lib-x/msgbus/go-sdk/msgbus/gen/msgbus/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// NodeID identifies a msgbus node as tenant, business-logic name, and device.
type NodeID = msgbusv1.NodeId

// Message is the durable publish/subscribe envelope returned by msgbusd.
type Message = msgbusv1.MessageEnvelope

// FifoMessage is the envelope stored in a FIFO queue.
type FifoMessage = msgbusv1.FifoMessage

// TopicHead describes the latest known head for a topic and origin node.
type TopicHead = msgbusv1.TopicHead

// PeerSyncState reports persisted synchronization lag and failure details.
type PeerSyncState = msgbusv1.PeerSyncState

// ReadyResponse reports whether msgbusd can serve storage-backed operations.
type ReadyResponse = msgbusv1.ReadyResponse

// DefaultReplayLimit is used when a subscription does not set ReplayLimit.
const DefaultReplayLimit uint32 = 100

// Client wraps the msgbus gRPC client and optional default origin.
//
// Applications should normally connect to a local msgbusd instance on the same
// device or host. The daemon owns persistence and peer synchronization.
type Client struct {
	conn          *grpc.ClientConn
	rpc           msgbusv1.MsgbusServiceClient
	defaultOrigin *msgbusv1.NodeId
}

// Dial connects to target and waits until the gRPC channel is ready.
//
// If no grpc.DialOption is provided, Dial uses insecure transport credentials,
// which is suitable for local daemon connections. Pass TLS credentials when
// connecting across machines or networks.
func Dial(ctx context.Context, target string, opts ...grpc.DialOption) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("msgbus: nil context")
	}
	client, err := DialLazy(target, opts...)
	if err != nil {
		return nil, err
	}
	if err := client.waitReady(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

// DialLazy creates a client and starts connecting without waiting for readiness.
//
// This is useful when the application wants to control connection readiness or
// perform its first RPC with its own timeout policy.
func DialLazy(target string, opts ...grpc.DialOption) (*Client, error) {
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

// waitReady blocks until the underlying gRPC channel is ready or ctx expires.
func (c *Client) waitReady(ctx context.Context) error {
	for {
		state := c.conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		c.conn.Connect()
		if !c.conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return errors.New("msgbus: connection did not become ready")
		}
	}
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// WithDefaultOrigin sets the origin used when calls do not pass one explicitly.
//
// For writes, msgbusd only accepts the local daemon origin. In normal
// deployments this value should match the daemon's configured node ID.
func (c *Client) WithDefaultOrigin(origin *msgbusv1.NodeId) *Client {
	c.defaultOrigin = origin
	return c
}

// Health checks whether msgbusd is alive and responding to gRPC requests.
//
// Use Ready when the caller also needs to know whether the daemon can access
// its backing store.
func (c *Client) Health(ctx context.Context) (string, error) {
	resp, err := c.rpc.Health(ctx, &msgbusv1.HealthRequest{})
	if err != nil {
		return "", err
	}
	return resp.Status, nil
}

// Ready checks whether msgbusd can serve storage-backed operations.
func (c *Client) Ready(ctx context.Context) (*msgbusv1.ReadyResponse, error) {
	return c.rpc.Ready(ctx, &msgbusv1.ReadyRequest{})
}

// Publish stores a message without headers.
//
// The daemon assigns the next head ID for the message's topic and origin.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) (*msgbusv1.MessageEnvelope, error) {
	return c.PublishWithHeaders(ctx, topic, payload, nil)
}

// PublishWithHeaders stores a message with application metadata headers.
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

// Fetch returns stored messages for a topic and origin starting at fromHeadID.
//
// If origin is nil, the client's default origin is used. A limit of zero lets
// msgbusd choose its default page size.
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

// Subscribe opens a replay-then-live subscription with default options.
//
// The returned stream ends on transport failures. Use SubscribeIterator when
// the caller wants automatic retry and resume behavior.
func (c *Client) Subscribe(ctx context.Context, topic string, origin *msgbusv1.NodeId, fromHeadID uint64) (msgbusv1.MsgbusService_SubscribeClient, error) {
	return c.SubscribeWithOptions(ctx, topic, SubscribeOptions{
		Origin:     origin,
		FromHeadID: fromHeadID,
	})
}

// SubscribeOptions configures replay and reconnect behavior for subscriptions.
type SubscribeOptions struct {
	// Origin selects the source node to read from. If nil, Client's default
	// origin is used.
	Origin *msgbusv1.NodeId
	// FromHeadID is the first head ID to deliver. Zero defaults to one.
	FromHeadID uint64
	// ReplayLimit caps how many stored messages msgbusd replays before live
	// delivery starts. Zero uses DefaultReplayLimit.
	ReplayLimit uint32
	// ReconnectDelay is used by SubscribeIterator between retry attempts. Zero
	// uses a short SDK default.
	ReconnectDelay time.Duration
}

// SubscribeWithOptions opens a replay-then-live subscription stream.
func (c *Client) SubscribeWithOptions(ctx context.Context, topic string, opts SubscribeOptions) (msgbusv1.MsgbusService_SubscribeClient, error) {
	opts = c.defaultSubscribeOptions(opts)
	return c.rpc.Subscribe(ctx, &msgbusv1.SubscribeRequest{
		Topic:       topic,
		Origin:      opts.Origin,
		FromHeadId:  opts.FromHeadID,
		ReplayLimit: opts.ReplayLimit,
	})
}

// SubscribeIterator returns a reconnecting subscription helper.
//
// The iterator tracks the next expected head ID and reopens the stream after
// retryable errors or EOF. The provided context controls the lifetime of
// reconnect attempts and Recv calls.
func (c *Client) SubscribeIterator(ctx context.Context, topic string, opts SubscribeOptions) *SubscribeIterator {
	if ctx == nil {
		ctx = context.Background()
	}
	opts = c.defaultSubscribeOptions(opts)
	return &SubscribeIterator{
		ctx:     ctx,
		client:  c,
		topic:   topic,
		options: opts,
	}
}

// defaultSubscribeOptions applies SDK defaults without mutating the caller's options.
func (c *Client) defaultSubscribeOptions(opts SubscribeOptions) SubscribeOptions {
	if opts.Origin == nil {
		opts.Origin = c.defaultOrigin
	}
	if opts.FromHeadID == 0 {
		opts.FromHeadID = 1
	}
	if opts.ReplayLimit == 0 {
		opts.ReplayLimit = DefaultReplayLimit
	}
	if opts.ReconnectDelay == 0 {
		opts.ReconnectDelay = 250 * time.Millisecond
	}
	return opts
}

// SubscribeIterator receives messages from a subscription with reconnect support.
//
// It is not safe for concurrent Recv calls.
type SubscribeIterator struct {
	ctx     context.Context
	client  *Client
	topic   string
	options SubscribeOptions
	stream  msgbusv1.MsgbusService_SubscribeClient
}

// Recv returns the next subscribed message.
//
// Retryable stream failures are handled by reopening the subscription from the
// next unsent head ID. Non-retryable errors are returned to the caller.
func (it *SubscribeIterator) Recv() (*msgbusv1.MessageEnvelope, error) {
	for {
		if it.stream == nil {
			stream, err := it.client.SubscribeWithOptions(it.ctx, it.topic, it.options)
			if err != nil {
				if isRetryable(err) {
					if err := it.waitReconnect(); err != nil {
						return nil, err
					}
					continue
				}
				return nil, err
			}
			it.stream = stream
		}

		resp, err := it.stream.Recv()
		if err == nil {
			if resp.Message == nil {
				return nil, errors.New("msgbus: empty subscribe response")
			}
			it.options.FromHeadID = resp.Message.HeadId + 1
			return resp.Message, nil
		}
		if errors.Is(err, io.EOF) || isRetryable(err) {
			it.stream = nil
			if err := it.waitReconnect(); err != nil {
				return nil, err
			}
			continue
		}
		return nil, err
	}
}

// waitReconnect waits before the next subscription reconnect attempt.
func (it *SubscribeIterator) waitReconnect() error {
	timer := time.NewTimer(it.options.ReconnectDelay)
	defer timer.Stop()
	select {
	case <-it.ctx.Done():
		return it.ctx.Err()
	case <-timer.C:
		return nil
	}
}

// GetHead returns the current head for a topic and origin.
func (c *Client) GetHead(ctx context.Context, topic string, origin *msgbusv1.NodeId) (*msgbusv1.GetHeadResponse, error) {
	if origin == nil {
		origin = c.defaultOrigin
	}
	return c.rpc.GetHead(ctx, &msgbusv1.GetHeadRequest{
		Topic:  topic,
		Origin: origin,
	})
}

// ListHeads lists all topic heads known to the local daemon.
func (c *Client) ListHeads(ctx context.Context) ([]*msgbusv1.TopicHead, error) {
	resp, err := c.rpc.ListHeads(ctx, &msgbusv1.ListHeadsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Heads, nil
}

// ListPeerSyncStates lists persisted peer synchronization lag and failures.
func (c *Client) ListPeerSyncStates(ctx context.Context) ([]*msgbusv1.PeerSyncState, error) {
	resp, err := c.rpc.ListPeerSyncStates(ctx, &msgbusv1.ListPeerSyncStatesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.States, nil
}

// DeleteRange marks a range of messages as deleted.
//
// Deletions are tombstones, so head ordering is preserved and delete markers
// can replicate to peers.
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

// EnqueueFIFO stores a FIFO message without headers.
//
// FIFO queues require consumers to ack or reject the front message first.
func (c *Client) EnqueueFIFO(ctx context.Context, queue string, topic string, target *msgbusv1.NodeId, payload []byte) (*msgbusv1.FifoMessage, error) {
	return c.EnqueueFIFOWithHeaders(ctx, queue, topic, target, payload, nil)
}

// EnqueueFIFOWithHeaders stores a FIFO message with application headers.
func (c *Client) EnqueueFIFOWithHeaders(ctx context.Context, queue string, topic string, target *msgbusv1.NodeId, payload []byte, headers map[string]string) (*msgbusv1.FifoMessage, error) {
	resp, err := c.rpc.EnqueueFifo(ctx, &msgbusv1.EnqueueFifoRequest{
		Queue:   queue,
		Topic:   topic,
		Source:  c.defaultOrigin,
		Target:  target,
		Payload: payload,
		Headers: headers,
	})
	if err != nil {
		return nil, err
	}
	if resp.Message == nil {
		return nil, errors.New("msgbus: empty fifo response")
	}
	return resp.Message, nil
}

// PeekFIFO returns the current front message of a FIFO queue without removing it.
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

// AckFIFO acknowledges the current front FIFO message and removes it.
//
// The returned bool is false when messageID is not the current front message.
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

// isRetryable reports whether a subscription error should reopen the stream.
func isRetryable(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Aborted || code == codes.Canceled
}

// RejectFIFO rejects the current front FIFO message and increments attempts.
//
// The returned bool is false when messageID is not the current front message.
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
