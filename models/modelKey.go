package models

import (
	"fmt"
	"sync"
	"time"
)

type ModelParams struct {
	PredIn    int
	PredOut   int
	Baseline  int
	UpdatedAt time.Time
}

type ModelKey struct {
	PreProModel  string
	PredictModel string
	AssessModel  string
}

type ModelParamsCache struct {
	cache  map[string]ModelParams
	mu     sync.RWMutex
	maxAge time.Duration
}

func NewModelParamsCache(maxAge time.Duration) *ModelParamsCache {
	return &ModelParamsCache{
		cache:  make(map[string]ModelParams),
		maxAge: maxAge,
	}
}

func (mk ModelKey) generateKey() string {
	return fmt.Sprintf("%s:%s:%s", mk.PreProModel, mk.PredictModel, mk.AssessModel)
}

func (c *ModelParamsCache) Get(key ModelKey) (ModelParams, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cacheKey := key.generateKey()
	params, exists := c.cache[cacheKey]
	if !exists {
		return ModelParams{}, false
	}

	if time.Since(params.UpdatedAt) > c.maxAge {
		delete(c.cache, cacheKey)
		return ModelParams{}, false
	}

	return params, true
}

func (c *ModelParamsCache) Set(key ModelKey, params ModelParams) {
	c.mu.Lock()
	defer c.mu.Unlock()

	params.UpdatedAt = time.Now()
	c.cache[key.generateKey()] = params
}

func (c *ModelParamsCache) Delete(key ModelKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.cache, key.generateKey())
}

func (c *ModelParamsCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]ModelParams)
}
