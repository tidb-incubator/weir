package namespace

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNamespaceRateLimiter_Limit(t *testing.T) {
	ctx := context.Background()
	key1 := "hello"
	key2 := "world"
	rateLimiter := NewNamespaceRateLimiter("namespace", 2)
	require.NoError(t, rateLimiter.Limit(ctx, key1))
	require.NoError(t, rateLimiter.Limit(ctx, key1))
	require.Error(t, rateLimiter.Limit(ctx, key1))
	require.NoError(t, rateLimiter.Limit(ctx, key2))
	time.Sleep(time.Second)
	require.NoError(t, rateLimiter.Limit(ctx, key1))
}

func TestNamespaceRateLimiter_ZeroThreshold(t *testing.T) {
	ctx := context.Background()
	key1 := "hello"
	rateLimiter := NewNamespaceRateLimiter("namespace", 0)
	require.NoError(t, rateLimiter.Limit(ctx, key1))
	require.NoError(t, rateLimiter.Limit(ctx, key1))
}
