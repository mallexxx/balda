package main

import (
	"errors"
	"fmt"
	"os"
)

func main() {
	if err := Execute(); err != nil {
		var unprintedErr *unprintedCLIError
		if errors.As(err, &unprintedErr) {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		var exitCoder interface{ ExitCode() int }
		if errors.As(err, &exitCoder) {
			os.Exit(exitCoder.ExitCode())
		}
		os.Exit(1)
	}
}
