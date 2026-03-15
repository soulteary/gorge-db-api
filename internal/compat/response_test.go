package compat

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestRespondOK(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	data := map[string]string{"key": "value"}
	if err := RespondOK(c, data); err != nil {
		t.Fatalf("RespondOK error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != nil {
		t.Error("error should be nil in OK response")
	}
	if resp.Data == nil {
		t.Error("data should not be nil")
	}
}

func TestRespondOKNilData(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := RespondOK(c, nil); err != nil {
		t.Fatalf("RespondOK error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRespondError(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	dbErr := &DBError{Code: ErrReadonlyWrite, Message: "cannot write"}
	if err := RespondError(c, dbErr); err != nil {
		t.Fatalf("RespondError error: %v", err)
	}

	if rec.Code != 409 {
		t.Errorf("expected 409, got %d", rec.Code)
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("error should not be nil")
	}
	if resp.Error.Code != ErrReadonlyWrite {
		t.Errorf("expected code %s, got %s", ErrReadonlyWrite, resp.Error.Code)
	}
	if resp.Error.Message != "cannot write" {
		t.Errorf("expected message 'cannot write', got %q", resp.Error.Message)
	}
	if resp.Data != nil {
		t.Error("data should be nil in error response")
	}
}

func TestRespondError503(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	dbErr := &DBError{Code: ErrAllUnreachable, Message: "all down"}
	if err := RespondError(c, dbErr); err != nil {
		t.Fatalf("RespondError error: %v", err)
	}
	if rec.Code != 503 {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

func TestRespondList(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	data := []string{"a", "b"}
	cursor := &Cursor{After: "abc", Limit: 10}
	if err := RespondList(c, data, cursor); err != nil {
		t.Fatalf("RespondList error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Data == nil {
		t.Error("data should not be nil")
	}
	if resp.Cursor == nil {
		t.Fatal("cursor should not be nil")
	}
	if resp.Cursor.After != "abc" {
		t.Errorf("cursor.After = %q, want abc", resp.Cursor.After)
	}
	if resp.Cursor.Limit != 10 {
		t.Errorf("cursor.Limit = %d, want 10", resp.Cursor.Limit)
	}
}

func TestRespondListNilCursor(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := RespondList(c, []int{1, 2}, nil); err != nil {
		t.Fatalf("RespondList error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Cursor != nil {
		t.Error("cursor should be nil when not provided")
	}
}

func TestAPIResponseJSONStructure(t *testing.T) {
	resp := APIResponse{
		Data: "hello",
		Error: &APIError{
			Code:    ErrQuery,
			Message: "bad query",
		},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["data"]; !ok {
		t.Error("JSON should have 'data' key")
	}
	if _, ok := m["error"]; !ok {
		t.Error("JSON should have 'error' key")
	}
}

func TestCursorJSONOmitEmpty(t *testing.T) {
	c := Cursor{}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	if s != "{}" {
		t.Errorf("empty cursor should marshal to {}, got %s", s)
	}

	c2 := Cursor{After: "x", Limit: 5}
	b2, _ := json.Marshal(c2)
	var m map[string]any
	_ = json.Unmarshal(b2, &m)
	if _, ok := m["before"]; ok {
		t.Error("before should be omitted when empty")
	}
}
