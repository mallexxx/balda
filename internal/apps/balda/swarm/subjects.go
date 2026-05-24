package swarm

import (
	"encoding/base32"
	"strings"
)

var subjectEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

func wakeSubject(addr ActorAddress) (string, error) {
	if _, err := addr.MailboxID(); err != nil {
		return "", err
	}
	target := strings.ToLower(strings.TrimSpace(addr.Target))
	encodedKey := strings.ToLower(subjectEncoding.EncodeToString([]byte(strings.TrimSpace(addr.Key))))
	return "balda.actor." + target + "." + encodedKey + ".wake", nil
}

func wakePattern() string {
	return "balda.actor.*.*.wake"
}

func mailboxWakePayload(addr ActorAddress) (string, error) {
	return addr.MailboxID()
}
