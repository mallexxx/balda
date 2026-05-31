package auth

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const tokenChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenerateOwnerToken generates a random owner token.
func GenerateOwnerToken() (string, error) {
	const length = 32
	result := make([]byte, length)
	for i := range result {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(tokenChars))))
		if err != nil {
			return "", fmt.Errorf("generate random number: %w", err)
		}
		result[i] = tokenChars[num.Int64()]
	}
	return string(result), nil
}
