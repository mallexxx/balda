package actors

import (
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/pkg/actorlayer"
)

func DeliveryEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return AgentReplyDeliveryEnvelope(taskID, from, locator, text, dedupeSuffix)
}

func AgentReplyDeliveryEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return deliverycmd.AgentReplyEnvelope(taskID, from, locator, text, dedupeSuffix)
}

func PlainDeliveryEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return deliverycmd.PlainEnvelope(taskID, from, locator, text, dedupeSuffix)
}

func MarkdownDeliveryEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	text string,
	dedupeSuffix string,
) (actorlayer.Envelope, error) {
	return deliverycmd.MarkdownEnvelope(taskID, from, locator, text, dedupeSuffix)
}

func DraftPlainDeliveryEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	draftID int,
	text string,
) (actorlayer.Envelope, error) {
	return deliverycmd.DraftPlainEnvelope(taskID, from, locator, draftID, text)
}

func ChatActionDeliveryEnvelope(
	taskID string,
	from actorlayer.ActorAddress,
	locator baldasession.SessionLocator,
	action string,
) (actorlayer.Envelope, error) {
	return deliverycmd.ChatActionEnvelope(taskID, from, locator, action)
}

func validateDeliveryPayload(payload DeliveryPayload) error {
	return deliverycmd.Validate(payload)
}
