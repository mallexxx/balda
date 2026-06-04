package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	"github.com/normahq/balda/internal/apps/balda/locatorref"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
)

const (
	envelopeTargetAlias   = "alias"
	envelopeAliasOwner    = "owner"
	envelopeTargetLocator = "locator"
)

type envelopeTarget struct {
	Target string
	Key    string
}

type resolvedEnvelopeTarget struct {
	Locator baldasession.SessionLocator
	UserID  string
	TopicID int
}

func resolveEnvelopeTarget(
	_ context.Context,
	ownerStore *auth.OwnerStore,
	target envelopeTarget,
) (resolvedEnvelopeTarget, error) {
	targetKind := strings.ToLower(strings.TrimSpace(target.Target))
	key := strings.TrimSpace(target.Key)
	if targetKind == "" {
		return resolvedEnvelopeTarget{}, fmt.Errorf("envelope target is required")
	}
	if key == "" {
		return resolvedEnvelopeTarget{}, fmt.Errorf("envelope target key is required")
	}

	switch targetKind {
	case envelopeTargetAlias:
		if strings.ToLower(key) != envelopeAliasOwner {
			return resolvedEnvelopeTarget{}, fmt.Errorf("unsupported alias target %q", target.Key)
		}
		if ownerStore == nil {
			return resolvedEnvelopeTarget{}, fmt.Errorf("owner store is required")
		}
		owner := ownerStore.GetOwner()
		if owner == nil {
			return resolvedEnvelopeTarget{}, fmt.Errorf("owner is not registered")
		}
		if owner.UserID == 0 {
			return resolvedEnvelopeTarget{}, fmt.Errorf("owner.user_id is required")
		}
		if owner.ChatID == 0 {
			return resolvedEnvelopeTarget{}, fmt.Errorf("owner.chat_id is required")
		}

		return resolvedEnvelopeTarget{
			Locator: baldatelegram.NewLocator(owner.ChatID, 0),
			UserID:  baldatelegram.UserID(owner.UserID),
			TopicID: 0,
		}, nil
	case envelopeTargetLocator:
		locator, err := locatorref.Parse(target.Key)
		if err != nil {
			return resolvedEnvelopeTarget{}, err
		}
		resolved := resolvedEnvelopeTarget{Locator: locator}
		if address, ok, decodeErr := baldatelegram.DecodeLocator(locator); decodeErr != nil {
			return resolvedEnvelopeTarget{}, decodeErr
		} else if ok {
			resolved.TopicID = address.TopicID
		}
		return resolved, nil
	default:
		return resolvedEnvelopeTarget{}, fmt.Errorf("unsupported envelope target %q", target.Target)
	}
}
