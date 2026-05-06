package handlers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/normahq/relay/internal/apps/relay/auth"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
)

type fakeRelayStartupTGClient struct {
	client.ClientWithResponsesInterface

	getMeResp  *client.GetMeResponse
	getMeErr   error
	getMeCalls int
}

const testRelayStartupBotUsername = "ValeraBot"

func (f *fakeRelayStartupTGClient) GetMeWithResponse(_ context.Context, _ ...client.RequestEditorFn) (*client.GetMeResponse, error) {
	f.getMeCalls++
	if f.getMeErr != nil {
		return nil, f.getMeErr
	}
	return f.getMeResp, nil
}

func TestRelayHandlerOnStart_FailsWhenGetMeTransportFails(t *testing.T) {
	handler := newRelayStartupHandlerForTest(t, &fakeRelayStartupTGClient{
		getMeErr: errors.New("network timeout"),
	}, "", zerolog.Nop())

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want startup failure")
	}
	if !strings.Contains(err.Error(), "resolve relay telegram bot identity") {
		t.Fatalf("onStart() error = %q, want bot identity context", err.Error())
	}
	if !strings.Contains(err.Error(), "network timeout") {
		t.Fatalf("onStart() error = %q, want getMe transport error", err.Error())
	}
}

func TestRelayHandlerOnStart_FailsWhenGetMeUnauthorized(t *testing.T) {
	handler := newRelayStartupHandlerForTest(t, &fakeRelayStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized"},
			JSON401: &client.ErrorResponse{
				Description: "Unauthorized",
			},
		},
	}, "", zerolog.Nop())

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want startup failure")
	}
	if !strings.Contains(err.Error(), "getMe unauthorized") {
		t.Fatalf("onStart() error = %q, want unauthorized context", err.Error())
	}
}

func TestRelayHandlerOnStart_FailsWhenGetMeUsernameEmpty(t *testing.T) {
	handler := newRelayStartupHandlerForTest(t, &fakeRelayStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id: 42,
				},
			},
		},
	}, "", zerolog.Nop())

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want startup failure")
	}
	if !strings.Contains(err.Error(), "empty username") {
		t.Fatalf("onStart() error = %q, want empty username error", err.Error())
	}
}

func TestRelayHandlerOnStart_LoadsBotIdentityWhenGetMeSucceeds(t *testing.T) {
	username := testRelayStartupBotUsername
	tgClient := &fakeRelayStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id:       7791683989,
					Username: &username,
				},
			},
		},
	}
	handler := newRelayStartupHandlerForTest(t, tgClient, "", zerolog.Nop())

	if err := handler.onStart(context.Background()); err != nil {
		t.Fatalf("onStart() error = %v", err)
	}
	if tgClient.getMeCalls != 1 {
		t.Fatalf("getMe calls = %d, want 1", tgClient.getMeCalls)
	}
	gotBotID, gotUsername := handler.getBotIdentity()
	if gotBotID != 7791683989 {
		t.Fatalf("bot user id = %d, want 7791683989", gotBotID)
	}
	if gotUsername != testRelayStartupBotUsername {
		t.Fatalf("bot username = %q, want %s", gotUsername, testRelayStartupBotUsername)
	}
}

func TestRelayHandlerOnStart_LogsOwnerAuthWhenOwnerUnknown(t *testing.T) {
	username := testRelayStartupBotUsername
	tgClient := &fakeRelayStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id:       7791683989,
					Username: &username,
				},
			},
		},
	}

	var buf bytes.Buffer
	handler := newRelayStartupHandlerForTest(t, tgClient, "owner-token", zerolog.New(&buf))

	if err := handler.onStart(context.Background()); err != nil {
		t.Fatalf("onStart() error = %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "/start owner=owner-token") {
		t.Fatalf("startup log missing auth command, output=%q", output)
	}
	if !strings.Contains(output, "https://t.me/"+testRelayStartupBotUsername+"?start=owner_owner-token") {
		t.Fatalf("startup log missing auth url, output=%q", output)
	}
}

func TestRelayHandlerOnStart_DoesNotLogOwnerAuthWhenOwnerRegistered(t *testing.T) {
	username := testRelayStartupBotUsername
	tgClient := &fakeRelayStartupTGClient{
		getMeResp: &client.GetMeResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
			JSON200: &struct {
				Ok     client.GetMe200Ok `json:"ok"`
				Result client.User       `json:"result"`
			}{
				Ok: true,
				Result: client.User{
					Id:       7791683989,
					Username: &username,
				},
			},
		},
	}

	var buf bytes.Buffer
	handler := newRelayStartupHandlerForTest(t, tgClient, "owner-token", zerolog.New(&buf))
	if _, err := handler.ownerStore.RegisterOwner(42, 0, "owner", "Relay", "Owner", false); err != nil {
		t.Fatalf("RegisterOwner() error = %v", err)
	}

	err := handler.onStart(context.Background())
	if err == nil {
		t.Fatal("onStart() error = nil, want missing chat id error")
	}
	if !strings.Contains(err.Error(), "owner.chat_id is required") {
		t.Fatalf("onStart() error = %q, want missing chat id error", err.Error())
	}

	output := buf.String()
	if strings.Contains(output, "/start owner=owner-token") {
		t.Fatalf("startup log unexpectedly included auth command, output=%q", output)
	}
	if strings.Contains(output, "https://t.me/"+testRelayStartupBotUsername+"?start=owner_owner-token") {
		t.Fatalf("startup log unexpectedly included auth url, output=%q", output)
	}
}

func newRelayStartupHandlerForTest(t *testing.T, tgClient client.ClientWithResponsesInterface, authToken string, logger zerolog.Logger) *RelayHandler {
	t.Helper()

	ownerStore, err := auth.NewOwnerStore(&fakeOwnerKVStore{})
	if err != nil {
		t.Fatalf("new owner store: %v", err)
	}

	return &RelayHandler{
		ownerStore: ownerStore,
		tgClient:   tgClient,
		authToken:  authToken,
		logger:     logger,
	}
}
