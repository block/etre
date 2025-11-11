package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/square/etre"
	"github.com/square/etre/api"
	"github.com/square/etre/app"
	"github.com/square/etre/auth"
	"github.com/square/etre/config"
	"github.com/square/etre/entity"
	"github.com/square/etre/metrics"
	srv "github.com/square/etre/server"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

func TestGetEntityTypes(t *testing.T) {
	tests := []struct {
		name        string
		entityTypes []string
		expectTypes []string
	}{
		{
			name:        "Single entity type",
			entityTypes: []string{"nodes"},
			expectTypes: []string{"nodes"},
		},
		{
			name:        "Multiple entity types",
			entityTypes: []string{"nodes", "racks", "hosts"},
			expectTypes: []string{"nodes", "racks", "hosts"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up the server with custom entity types
			config := defaultConfig
			server := setupWithValidator(t, config, mock.EntityStore{}, entity.NewValidator(tt.entityTypes))
			defer server.ts.Close()

			// Set up the request URL
			etreurl := server.url + etre.API_ROOT + "/entity-types"

			// Make the HTTP call
			var gotTypes []string
			statusCode, err := test.MakeHTTPRequest("GET", etreurl, nil, &gotTypes)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, statusCode, "response status = %d, expected %d, url %s", statusCode, http.StatusOK, etreurl)

			// Make sure we got the expected entity types
			assert.Equal(t, tt.expectTypes, gotTypes)
		})
	}
}

func setupWithValidator(t *testing.T, cfg config.Config, store mock.EntityStore, validator entity.Validator) *server {
	etre.DebugEnabled = true

	server := &server{
		store:           store,
		cfg:             cfg,
		auth:            &mock.AuthRecorder{},
		cdcStore:        &mock.CDCStore{},
		streamerFactory: &mock.StreamerFactory{},
		metricsrec:      mock.NewMetricsRecorder(),
		sysmetrics:      mock.NewMetricsRecorder(),
	}

	acls, err := srv.MapConfigACLRoles(cfg.Security.ACL)
	require.NoError(t, err, "invalid Config.ACL: %s", err)

	ms := metrics.NewMemoryStore()
	mf := metrics.GroupFactory{Store: ms}
	sm := metrics.NewSystemMetrics()

	appCtx := app.Context{
		Config:          server.cfg,
		EntityStore:     server.store,
		EntityValidator: validator,
		Auth:            auth.NewManager(acls, server.auth),
		MetricsStore:    ms,
		MetricsFactory:  mock.NewMetricsFactory(mf, server.metricsrec),
		StreamerFactory: server.streamerFactory,
		SystemMetrics:   mock.NewSystemMetrics(sm, server.sysmetrics),
	}
	server.api = api.NewAPI(appCtx)
	server.ts = httptest.NewServer(server.api)

	u, err := url.Parse(server.ts.URL)
	require.NoError(t, err)

	server.url = fmt.Sprintf("http://%s", u.Host)

	return server
}
