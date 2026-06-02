package handlers

import (
	"context"
	"fmt"

	"github.com/normahq/balda/internal/apps/balda/actors"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	"github.com/normahq/balda/internal/apps/balda/swarm"
)

var (
	baldaHandlerActorAddress   = swarm.ActorAddress{Target: "handler", Key: "balda"}
	commandHandlerActorAddress = swarm.ActorAddress{Target: "handler", Key: "command"}
	userHandlerActorAddress    = swarm.ActorAddress{Target: "handler", Key: "user"}
	startHandlerActorAddress   = swarm.ActorAddress{Target: "handler", Key: "start"}
)

func dispatchOutbound(ctx context.Context, dispatcher swarm.ActorDispatcher, env swarm.Envelope) error {
	if dispatcher == nil {
		return fmt.Errorf("swarm runtime is unavailable")
	}
	_, err := dispatcher.Dispatch(ctx, env)
	return err
}

func sendPlain(ctx context.Context, dispatcher swarm.ActorDispatcher, from swarm.ActorAddress, locator baldasession.SessionLocator, text string) error {
	env, err := actors.PlainDeliveryEnvelope("", from, locator, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendMarkdown(ctx context.Context, dispatcher swarm.ActorDispatcher, from swarm.ActorAddress, locator baldasession.SessionLocator, text string) error {
	env, err := actors.MarkdownDeliveryEnvelope("", from, locator, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendAgentReply(ctx context.Context, dispatcher swarm.ActorDispatcher, from swarm.ActorAddress, locator baldasession.SessionLocator, text string) error {
	env, err := actors.AgentReplyDeliveryEnvelope("", from, locator, text, "")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendDraftPlain(ctx context.Context, dispatcher swarm.ActorDispatcher, from swarm.ActorAddress, locator baldasession.SessionLocator, draftID int, text string) error {
	env, err := actors.DraftPlainDeliveryEnvelope("", from, locator, draftID, text)
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}

func sendTyping(ctx context.Context, dispatcher swarm.ActorDispatcher, from swarm.ActorAddress, locator baldasession.SessionLocator) error {
	env, err := actors.ChatActionDeliveryEnvelope("", from, locator, "typing")
	if err != nil {
		return err
	}
	return dispatchOutbound(ctx, dispatcher, env)
}
