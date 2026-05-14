package telegram

import (
	"encoding/json"
	"fmt"
	"strings"

	relaysession "github.com/normahq/balda/internal/apps/balda/session"
	relaystate "github.com/normahq/balda/internal/apps/balda/state"
)

const telegramSessionIDPrefix = "tg"

// LocatorAddress is the Telegram-specific transport address payload.
type LocatorAddress struct {
	ChatID  int64 `json:"chat_id"`
	TopicID int   `json:"topic_id"`
}

// NewLocator builds a canonical session locator for Telegram transport.
func NewLocator(chatID int64, topicID int) relaysession.SessionLocator {
	address := LocatorAddress{ChatID: chatID, TopicID: topicID}
	raw, _ := json.Marshal(address)
	channelType := relaystate.ChannelTypeTelegram
	addressKey := fmt.Sprintf("%d:%d", chatID, topicID)
	addressJSON := string(raw)
	sessionID := fmt.Sprintf("%s-%d-%d", telegramSessionIDPrefix, chatID, topicID)

	locator, err := relaysession.NewSessionLocator(channelType, addressKey, addressJSON, sessionID)
	if err != nil {
		// Generated values are deterministic and should always validate.
		return relaysession.SessionLocator{
			ChannelType: channelType,
			AddressKey:  addressKey,
			AddressJSON: addressJSON,
			SessionID:   sessionID,
		}
	}
	return locator
}

// DecodeLocator decodes a Telegram locator payload from canonical session locator fields.
func DecodeLocator(locator relaysession.SessionLocator) (LocatorAddress, bool, error) {
	if strings.TrimSpace(locator.ChannelType) != relaystate.ChannelTypeTelegram {
		return LocatorAddress{}, false, nil
	}

	var address LocatorAddress
	if err := json.Unmarshal([]byte(locator.AddressJSON), &address); err != nil {
		return LocatorAddress{}, true, fmt.Errorf("decode telegram address: %w", err)
	}
	return address, true, nil
}

// UserID returns a Telegram-backed ADK transport user identifier.
func UserID(userID int64) string {
	return fmt.Sprintf("%s-%d", telegramSessionIDPrefix, userID)
}

