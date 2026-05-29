package swarm

import (
	"errors"
	"testing"
)

func TestClassifyErrorKinds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want ErrorKind
	}{
		{name: "retryable alias", err: RetryableError(errors.New("retry")), want: ErrorKindTransient},
		{name: "permanent", err: PermanentError(errors.New("perm")), want: ErrorKindPermanent},
		{name: "canceled", err: CanceledError(errors.New("cancel")), want: ErrorKindCanceled},
		{name: "decode", err: DecodeError(errors.New("decode")), want: ErrorKindDecode},
		{name: "external delivery", err: ExternalDeliveryError(errors.New("send failed")), want: ErrorKindExternalDelivery},
		{name: "fallback transient", err: errors.New("plain error"), want: ErrorKindTransient},
		{name: "nil", err: nil, want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyError(tc.err); got != tc.want {
				t.Fatalf("ClassifyError() = %q, want %q", got, tc.want)
			}
		})
	}
}
