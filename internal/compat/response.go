package compat

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// APIResponse is the unified JSON envelope for all HTTP responses.
type APIResponse struct {
	Data   any       `json:"data,omitempty"`
	Error  *APIError `json:"error,omitempty"`
	Cursor *Cursor   `json:"cursor,omitempty"`
}

type APIError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details any       `json:"details,omitempty"`
}

type Cursor struct {
	After  string `json:"after,omitempty"`
	Before string `json:"before,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

func RespondOK(c echo.Context, data any) error {
	return c.JSON(http.StatusOK, &APIResponse{Data: data})
}

func RespondError(c echo.Context, err *DBError) error {
	return c.JSON(err.HTTPStatus(), &APIResponse{
		Error: &APIError{
			Code:    err.Code,
			Message: err.Message,
			Details: err.Details,
		},
	})
}

func RespondList(c echo.Context, data any, cursor *Cursor) error {
	return c.JSON(http.StatusOK, &APIResponse{Data: data, Cursor: cursor})
}
