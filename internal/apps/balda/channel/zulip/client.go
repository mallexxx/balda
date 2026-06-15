package zulip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultHTTPTimeout = 15 * time.Second
const eventsHTTPTimeout = 120 * time.Second // > 90s Zulip long-poll window

// Client is a low-level Zulip REST client.
type Client struct {
	baseURL  string
	botEmail string
	apiKey   string
	http     *http.Client
}

// sendMessageResult holds the Zulip API response for a sent message.
type sendMessageResult struct {
	Result string `json:"result"`
	Msg    string `json:"msg"`
	ID     int    `json:"id"`
}

// NewClient creates a new Zulip API client.
func NewClient(baseURL, botEmail, apiKey string) *Client {
	return &Client{
		baseURL:  strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		botEmail: strings.TrimSpace(botEmail),
		apiKey:   strings.TrimSpace(apiKey),
		http:     &http.Client{Timeout: defaultHTTPTimeout},
	}
}

// SendStreamMessage sends a message to a Zulip stream topic.
// Returns the Zulip message ID on success.
func (c *Client) SendStreamMessage(
	ctx context.Context,
	streamID int,
	topic string,
	content string,
) (int, error) {
	toJSON, _ := json.Marshal(streamID)
	form := url.Values{}
	form.Set("type", "stream")
	form.Set("to", string(toJSON))
	form.Set("topic", topic)
	form.Set("content", content)
	return c.postMessage(ctx, form)
}

// SendDirectMessage sends a direct message to a Zulip user.
// Returns the Zulip message ID on success.
func (c *Client) SendDirectMessage(
	ctx context.Context,
	userID int,
	content string,
) (int, error) {
	toJSON, _ := json.Marshal([]int{userID})
	form := url.Values{}
	form.Set("type", "direct")
	form.Set("to", string(toJSON))
	form.Set("content", content)
	return c.postMessage(ctx, form)
}

// SendStreamTyping sends a typing indicator to a stream topic.
func (c *Client) SendStreamTyping(
	ctx context.Context,
	streamID int,
	topic string,
) error {
	type streamTarget struct {
		StreamID int    `json:"stream_id"`
		Topic    string `json:"topic"`
	}
	toJSON, _ := json.Marshal([]streamTarget{{StreamID: streamID, Topic: topic}})
	form := url.Values{}
	form.Set("op", "start")
	form.Set("type", "stream")
	form.Set("to", string(toJSON))
	return c.postTyping(ctx, form)
}

// SendDirectTyping sends a typing indicator to a direct message conversation.
func (c *Client) SendDirectTyping(ctx context.Context, userID int) error {
	toJSON, _ := json.Marshal([]int{userID})
	form := url.Values{}
	form.Set("op", "start")
	form.Set("type", "direct")
	form.Set("to", string(toJSON))
	return c.postTyping(ctx, form)
}

func (c *Client) postMessage(ctx context.Context, form url.Values) (int, error) {
	body, err := c.post(ctx, "/api/v1/messages", form)
	if err != nil {
		return 0, err
	}
	var result sendMessageResult
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("decode zulip send message response: %w", err)
	}
	if result.Result != "success" {
		return 0, fmt.Errorf("zulip send message failed: %s", result.Msg)
	}
	return result.ID, nil
}

func (c *Client) postTyping(ctx context.Context, form url.Values) error {
	_, err := c.post(ctx, "/api/v1/typing", form)
	return err
}

// BotEmail returns the bot's email address.
func (c *Client) BotEmail() string { return c.botEmail }

// eventsQueueInfo holds the result of a successful events queue registration.
type eventsQueueInfo struct {
	QueueID     string
	LastEventID int
}

type eventsRegisterResponse struct {
	Result      string `json:"result"`
	Msg         string `json:"msg"`
	QueueID     string `json:"queue_id"`
	LastEventID int    `json:"last_event_id"`
}

// EventMessage is a single event from the Zulip Events API.
type EventMessage struct {
	ID      int              `json:"id"`
	Type    string           `json:"type"`
	Message EventMessageBody `json:"message"`
	Flags   []string         `json:"flags"`
}

// EventMessageBody is the message payload within an EventMessage.
type EventMessageBody struct {
	ID          int      `json:"id"`
	SenderID    int      `json:"sender_id"`
	SenderEmail string   `json:"sender_email"`
	Type        string   `json:"type"`
	StreamID    int      `json:"stream_id"`
	Subject     string   `json:"subject"`
	Content     string   `json:"content"`
	Flags       []string `json:"flags"`
}

type eventsGetResponse struct {
	Result string         `json:"result"`
	Msg    string         `json:"msg"`
	Events []EventMessage `json:"events"`
}

func (c *Client) registerEventsQueue(ctx context.Context) (eventsQueueInfo, error) {
	typesJSON, _ := json.Marshal([]string{"message"})
	form := url.Values{}
	form.Set("event_types", string(typesJSON))
	body, err := c.post(ctx, "/api/v1/register", form)
	if err != nil {
		return eventsQueueInfo{}, fmt.Errorf("register events queue: %w", err)
	}
	var resp eventsRegisterResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return eventsQueueInfo{}, fmt.Errorf("decode register response: %w", err)
	}
	if resp.Result != "success" {
		return eventsQueueInfo{}, fmt.Errorf("register events queue: %s", resp.Msg)
	}
	return eventsQueueInfo{QueueID: resp.QueueID, LastEventID: resp.LastEventID}, nil
}

func (c *Client) getEvents(ctx context.Context, queueID string, lastEventID int) ([]EventMessage, error) {
	params := url.Values{}
	params.Set("queue_id", queueID)
	params.Set("last_event_id", fmt.Sprintf("%d", lastEventID))
	params.Set("dont_block", "false")

	endpoint := c.baseURL + "/api/v1/events?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build events request: %w", err)
	}
	req.SetBasicAuth(c.botEmail, c.apiKey)

	longPollHTTP := &http.Client{Timeout: eventsHTTPTimeout}
	resp, err := longPollHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("events GET: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read events response: %w", err)
	}
	if resp.StatusCode == http.StatusBadRequest {
		return nil, fmt.Errorf("events queue expired or invalid: %s", string(raw))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("events GET HTTP %d: %s", resp.StatusCode, string(raw))
	}

	var evResp eventsGetResponse
	if err := json.Unmarshal(raw, &evResp); err != nil {
		return nil, fmt.Errorf("decode events response: %w", err)
	}
	if evResp.Result != "success" {
		return nil, fmt.Errorf("events GET: %s", evResp.Msg)
	}
	return evResp.Events, nil
}

func (c *Client) post(
	ctx context.Context,
	path string,
	form url.Values,
) ([]byte, error) {
	endpoint := c.baseURL + path
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf("build zulip request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.botEmail, c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("zulip request to %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read zulip response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"zulip %s returned HTTP %d: %s",
			path, resp.StatusCode, string(body),
		)
	}
	return body, nil
}
