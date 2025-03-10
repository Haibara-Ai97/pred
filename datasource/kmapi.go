package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"pred/sql"
	"time"
)

type HTTPDataSource struct {
	baseURL      string
	client       *http.Client
	tokenManager *TokenManager
}

type APIResponse struct {
	Success bool          `json:"success"`
	Data    []*sql.YSBase `json:"data"`
	Error   string        `json:"error,omitempty"`
}

func NewHTTPDataSource(baseURL string) *HTTPDataSource {
	return &HTTPDataSource{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (h *HTTPDataSource) GetPrepareData(ctx context.Context, tableName, pointName string,
	startTime, endTime time.Time) ([]*sql.YSBase, error) {
	url := fmt.Sprintf("/ToBeDone")

	return h.fetchData(ctx, url)
}

func (h *HTTPDataSource) GetOriginData(ctx context.Context, tableName, pointName string,
	startTime, endTime time.Time) ([]*sql.YSBase, error) {
	url := fmt.Sprintf("/ToBeDone")

	return h.fetchData(ctx, url)
}

func (h *HTTPDataSource) GetLastOriginData(ctx context.Context, tableName, pointName string,
	startTime, endTime time.Time) ([]*sql.YSBase, error) {
	url := fmt.Sprintf("/ToBeDone")

	return h.fetchData(ctx, url)
}

func (h *HTTPDataSource) GetMaxTime(ctx context.Context, tableName string) (*sql.YSBase, error) {
	url := fmt.Sprintf("/ToBeDone")
	result, err := h.fetchData(ctx, url)
	return result[0], err
}

func (h *HTTPDataSource) GetPointType(ctx context.Context, pointName string) (string, error) {
	return "ToBeDone", nil
}

func (h *HTTPDataSource) fetchData(ctx context.Context, url string) ([]*sql.YSBase, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %v", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %v", err)
	}
	defer resp.Body.Close()

	var response APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %v", err)
	}

	if !response.Success {
		return nil, fmt.Errorf("api error: %v", response.Error)
	}

	return response.Data, nil
}
