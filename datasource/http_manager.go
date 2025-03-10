package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type HTTPManager struct {
	baseURL string
	client  *http.Client
	TokenManager *TokenManager
	dataSource *HTTPDataSource
	once sync.Once
}

func NewHTTPManager(baseURL string, headers map[string]string) *HTTPManager {
	client := &http.Client{Timeout: 10 * time.Second}

	manager := &HTTPManager{
		baseURL: baseURL,
		client:  client,
	}

	getTokenFn := func(ctx context.Context) (string, time.Time, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/auth/thirdPartyAuth/token", nil)
		if err != nil {
			return "", time.Time{}, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			return "", time.Time{}, err
		}
		defer resp.Body.Close()

		var tokenResp struct {
			Token string `json:"token"`
			ExpireTime time.Time `json:"expire_time"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
			return "", time.Time{}, err
		}

		return tokenResp.Token, tokenResp.ExpireTime, nil
	}

	manager.TokenManager = NewTokenManager(getTokenFn)
	return manager
}

func (m *HTTPManager) Initialize(ctx context.Context) error {
	_, err := m.TokenManager.GetToken(ctx)
	if err != nil {
		return fmt.Errorf("initialize token acquisition failed: %w", err)
	}

	m.once.Do(func() {
		m.dataSource = &HTTPDataSource{
			baseURL: m.baseURL,
			client:  m.client,
			tokenManager: m.TokenManager,
		}
	})

	return nil
}

func (m *HTTPManager) GetDataSource() DataSource {
	return m.dataSource
}