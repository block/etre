package api_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
	"github.com/square/etre/schema"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

func TestGetSchemas(t *testing.T) {
	tests := []struct {
		name       string
		entityType string
		config     schema.Config
		expect     schema.Config
	}{
		{
			name:       "No schemas configured",
			entityType: "nodes",
			expect:     schema.Config{Entities: map[string]schema.EntitySchema{"nodes": {}}}, // the server will return an empty schema for known types
		},
		{
			name: "No schemas configured - No type param",
		},
		{
			name:       "Schema configured - type param present",
			entityType: "nodes",
			config: schema.Config{
				Entities: map[string]schema.EntitySchema{
					"nodes": {
						Schema: &schema.Schema{
							Fields: []schema.Field{
								{Name: "hostname", Type: "string", Required: true},
								{Name: "status", Type: "string", Required: false},
							},
							AdditionalProperties: true,
							ValidationLevel:      "strict",
						},
					},
				},
			},
			expect: schema.Config{
				Entities: map[string]schema.EntitySchema{
					"nodes": {
						Schema: &schema.Schema{
							Fields: []schema.Field{
								{Name: "hostname", Type: "string", Required: true},
								{Name: "status", Type: "string", Required: false},
							},
							AdditionalProperties: true,
							ValidationLevel:      "strict",
						},
					},
				},
			},
		},
		{
			name: "Schema configured - no type param",
			config: schema.Config{
				Entities: map[string]schema.EntitySchema{
					"nodes": {
						Schema: &schema.Schema{
							Fields: []schema.Field{
								{Name: "hostname", Type: "string", Required: true},
							},
							AdditionalProperties: false,
						},
					},
					"racks": {
						Schema: &schema.Schema{
							Fields: []schema.Field{
								{Name: "rack_id", Type: "string", Required: true},
								{Name: "datacenter", Type: "string", Required: true},
							},
							ValidationLevel: "moderate",
						},
					},
				},
			},
			expect: schema.Config{
				Entities: map[string]schema.EntitySchema{
					"nodes": {
						Schema: &schema.Schema{
							Fields: []schema.Field{
								{Name: "hostname", Type: "string", Required: true},
							},
							AdditionalProperties: false,
						},
					},
					"racks": {
						Schema: &schema.Schema{
							Fields: []schema.Field{
								{Name: "rack_id", Type: "string", Required: true},
								{Name: "datacenter", Type: "string", Required: true},
							},
							ValidationLevel: "moderate",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up the server
			config := defaultConfig
			config.Schemas = tt.config
			server := setup(t, config, mock.EntityStore{})
			defer server.ts.Close()

			// Set up the request URL
			etreurl := server.url + etre.API_ROOT + "/schemas"
			if tt.entityType != "" {
				etreurl += "/" + tt.entityType
			}

			// Make the HTTP call
			var gotSchemas schema.Config
			statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotSchemas)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, statusCode, "response status = %d, expected %d, url %s", statusCode, http.StatusOK, etreurl)

			// Make sure we got the expected schemas
			assert.Equal(t, tt.expect, gotSchemas)
		})
	}
}
