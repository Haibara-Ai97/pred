package datasource

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type TokenManager struct {
	token string
	expireTime time.Time
	mutex sync.RWMutex
	refreshToken string
	getTokenFN func(ctx context.Context) (string, time.Time, error)
}

func NewTokenManager(getTokenFN func(context.Context) (string, time.Time, error)) *TokenManager {
	return &TokenManager{getTokenFN: getTokenFN}
}

func (tm *TokenManager) GetToken(ctx context.Context) (string, error) {
	tm.mutex.RLock()
	if tm.token != "" && time.Now().Before(tm.expireTime) {
		token := tm.token
		tm.mutex.RUnlock()
		return token, nil
	}
	tm.mutex.RUnlock()

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.token != "" && time.Now().Before(tm.expireTime) {
		return tm.token, nil
	}

	token, expireTime, err := tm.getTokenFN(ctx)
	if err != nil {
		return "", fmt.Errorf("refresh token failed: %v", err)
	}

	tm.token = token
	tm.expireTime = expireTime
	return token, nil
}