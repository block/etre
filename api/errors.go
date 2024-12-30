// Copyright 2017-2018, Square, Inc.

package api

import (
	"net/http"

	"github.com/square/etre"
)

// These are default API-level error responses that should not be modified.
// See api.go for how they're used. The API writes them back to clients. It
// uses the fields to create a custom etre.Error and set the HTTP status code.

var ErrDuplicateEntity = etre.Error{
	Type:       "duplicate-entity",
	HTTPStatus: http.StatusConflict,
	Message:    "cannot insert or update entity because identifying labels conflict with another entity",
}

var ErrNotFound = etre.Error{
	Type:       "entity-not-found",
	HTTPStatus: http.StatusNotFound,
	Message:    "entity not found",
}

var ErrMissingParam = etre.Error{
	Type:       "missing-param",
	HTTPStatus: http.StatusBadRequest,
	Message:    "missing parameter",
}

var ErrInvalidParam = etre.Error{
	Type:       "invalid-param",
	HTTPStatus: http.StatusBadRequest,
	Message:    "missing parameter",
}

var ErrInvalidQuery = etre.Error{
	Type:       "invalid-query",
	HTTPStatus: http.StatusBadRequest,
	Message:    "invalid query",
}

var ErrInternal = etre.Error{
	Type:       "internal-error",
	HTTPStatus: http.StatusInternalServerError,
	Message:    "internal server error",
}

var ErrCDCDisabled = etre.Error{
	Type:       "cdc-disabled",
	HTTPStatus: http.StatusNotImplemented,
	Message:    "CDC disabled",
}

var ErrNoContent = etre.Error{
	Message:    "no entities provided (PUT or POST with zero-length HTTP payload or JSON array)",
	Type:       "no-content",
	HTTPStatus: http.StatusBadRequest,
}

var ErrInvalidContent = etre.Error{
	Message:    "HTTP payload is not valid JSON: []etre.Entity for POST or etre.Entity for PUT",
	Type:       "invalid-content",
	HTTPStatus: http.StatusBadRequest,
}

var ErrEndpointNotFound = etre.Error{
	Message:    "API endpoint not found",
	Type:       "endpoint-not-found",
	HTTPStatus: http.StatusNotFound,
}
