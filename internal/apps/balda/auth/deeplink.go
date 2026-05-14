package auth

import (
	"fmt"
	"strings"
)

// BuildOwnerAuthCommand returns the /start command for owner bootstrap.
func BuildOwnerAuthCommand(ownerToken string) string {
	return fmt.Sprintf("/start owner=%s", strings.TrimSpace(ownerToken))
}

// BuildOwnerAuthURL returns the Telegram deeplink for owner bootstrap.
func BuildOwnerAuthURL(botUsername, ownerToken string) string {
	username := strings.TrimSpace(botUsername)
	if username == "" {
		username = "<bot_username>"
	}
	return fmt.Sprintf("https://t.me/%s?start=owner_%s", username, strings.TrimSpace(ownerToken))
}
