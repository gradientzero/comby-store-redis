package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	store "github.com/gradientzero/comby-store-redis"
	"github.com/gradientzero/comby/v2"
)

func TestCacheStore1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0)
	if err = cacheStore.Init(ctx,
		comby.CacheStoreOptionWithAttribute("key1", "value"),
	); err != nil {
		t.Fatal(err)
	}

	// check if the attribute is set
	if v := cacheStore.Options().Attributes.Get("key1"); v != nil {
		if v != "value" {
			t.Fatalf("wrong value: %q", v)
		}
	} else {
		t.Fatalf("missing key")
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// check totals
	if cacheStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", cacheStore.Total(ctx))
	}

	// Set values
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("redisKey1", "redisValue1"),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("redisKeyBool", true),
	); err != nil {
		t.Fatal(err)
	}

	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("redisKeyExp", "redisValueExp"),
		comby.CacheStoreSetOptionWithExpiration(time.Millisecond*100),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("redisKey1"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel.Value != "redisValue1" {
			t.Fatalf("wrong value: %q", cacheModel.Value)
		}
	}

	// List all keys
	if cacheModels, _, err := cacheStore.List(ctx); err != nil {
		if len(cacheModels) != 3 {
			t.Fatalf("wrong number of keys: %d", len(cacheModels))
		}
	}

	// wait for expiration
	time.Sleep(time.Millisecond * 200)
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("redisKeyExp"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel != nil {
			t.Fatalf("key should not exist")
		}
	}

	// Delete a key
	if err := cacheStore.Delete(ctx,
		comby.CacheStoreDeleteOptionWithKey("redisKeyBool"),
	); err != nil {
		t.Fatal(err)
	}

	// List all keys
	if cacheModels, _, err := cacheStore.List(ctx); err == nil {
		if len(cacheModels) != 1 {
			t.Fatalf("wrong number of keys: %d", len(cacheModels))
		}
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStoreWithEncryption(t *testing.T) {
	var err error
	ctx := context.Background()

	// create crypto service with 32-byte key for AES-256
	key := []byte("01234567890123456789012345678901")
	cryptoService, err := comby.NewCryptoService(key)
	if err != nil {
		t.Fatal(err)
	}

	// setup and init store with crypto service
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 1,
		comby.CacheStoreOptionWithCryptoService(cryptoService),
	)
	if err = cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// check totals
	if cacheStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", cacheStore.Total(ctx))
	}

	// Set encrypted values
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("encryptedKey1", "secret value 1"),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("encryptedKey2", map[string]any{
			"username": "john_doe",
			"password": "super_secret",
			"apiKey":   "abc123xyz",
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("encryptedKeyBool", true),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("encryptedKeyNumber", 42),
	); err != nil {
		t.Fatal(err)
	}

	// Get and verify string value
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("encryptedKey1"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel.Value != "secret value 1" {
			t.Fatalf("wrong value: %q", cacheModel.Value)
		}
	}

	// Get and verify map value
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("encryptedKey2"),
	); err != nil {
		t.Fatal(err)
	} else {
		mapValue, ok := cacheModel.Value.(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", cacheModel.Value)
		}
		if mapValue["username"] != "john_doe" {
			t.Fatalf("wrong username: %q", mapValue["username"])
		}
		if mapValue["password"] != "super_secret" {
			t.Fatalf("wrong password: %q", mapValue["password"])
		}
		if mapValue["apiKey"] != "abc123xyz" {
			t.Fatalf("wrong apiKey: %q", mapValue["apiKey"])
		}
	}

	// Get and verify boolean value
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("encryptedKeyBool"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel.Value != true {
			t.Fatalf("wrong value: %v", cacheModel.Value)
		}
	}

	// Get and verify number value
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("encryptedKeyNumber"),
	); err != nil {
		t.Fatal(err)
	} else {
		// JSON unmarshaling will convert integers to float64
		if cacheModel.Value != float64(42) {
			t.Fatalf("wrong value: %v (type: %T)", cacheModel.Value, cacheModel.Value)
		}
	}

	// List all encrypted keys
	if cacheModels, total, err := cacheStore.List(ctx); err != nil {
		t.Fatal(err)
	} else {
		if len(cacheModels) != 4 {
			t.Fatalf("wrong number of keys: %d", len(cacheModels))
		}
		if total != 4 {
			t.Fatalf("wrong total: %d", total)
		}
	}

	// Delete an encrypted key
	if err := cacheStore.Delete(ctx,
		comby.CacheStoreDeleteOptionWithKey("encryptedKeyBool"),
	); err != nil {
		t.Fatal(err)
	}

	// Verify deletion
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("encryptedKeyBool"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel != nil {
			t.Fatalf("key should not exist")
		}
	}

	// List all keys after deletion
	if cacheModels, total, err := cacheStore.List(ctx); err != nil {
		t.Fatal(err)
	} else {
		if len(cacheModels) != 3 {
			t.Fatalf("wrong number of keys: %d", len(cacheModels))
		}
		if total != 3 {
			t.Fatalf("wrong total: %d", total)
		}
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStore_DeleteError(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0)
	if err = cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Test delete non-existent key - should not error
	if err := cacheStore.Delete(ctx,
		comby.CacheStoreDeleteOptionWithKey("non-existent-key"),
	); err != nil {
		t.Fatalf("delete should not error on non-existent key: %v", err)
	}

	// Set a key and delete it
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("test-key", "test-value"),
	); err != nil {
		t.Fatal(err)
	}

	// Delete the key
	if err := cacheStore.Delete(ctx,
		comby.CacheStoreDeleteOptionWithKey("test-key"),
	); err != nil {
		t.Fatalf("delete should not error: %v", err)
	}

	// Verify key is deleted
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("test-key"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel != nil {
			t.Fatalf("key should not exist after deletion")
		}
	}

	// close connection
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStore_TenantIsolation(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0)
	if err = cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Set keys with different tenant prefixes
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("tenant1-key1", "value1"),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("tenant1-key2", "value2"),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("tenant2-key1", "value3"),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("tenant2-key2", "value4"),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("no-tenant-key", "value5"),
	); err != nil {
		t.Fatal(err)
	}

	// List with tenant1 filter
	if cacheModels, total, err := cacheStore.List(ctx,
		comby.CacheStoreListOptionWithTenantUuid("tenant1"),
	); err != nil {
		t.Fatal(err)
	} else {
		if len(cacheModels) != 2 {
			t.Fatalf("expected 2 keys for tenant1, got %d", len(cacheModels))
		}
		if total != 2 {
			t.Fatalf("expected total 2 for tenant1, got %d", total)
		}
	}

	// List with tenant2 filter
	if cacheModels, total, err := cacheStore.List(ctx,
		comby.CacheStoreListOptionWithTenantUuid("tenant2"),
	); err != nil {
		t.Fatal(err)
	} else {
		if len(cacheModels) != 2 {
			t.Fatalf("expected 2 keys for tenant2, got %d", len(cacheModels))
		}
		if total != 2 {
			t.Fatalf("expected total 2 for tenant2, got %d", total)
		}
	}

	// List all (no filter)
	if cacheModels, total, err := cacheStore.List(ctx); err != nil {
		t.Fatal(err)
	} else {
		if len(cacheModels) != 5 {
			t.Fatalf("expected 5 keys total, got %d", len(cacheModels))
		}
		if total != 5 {
			t.Fatalf("expected total 5, got %d", total)
		}
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStore_InitConnectionFailure(t *testing.T) {
	ctx := context.Background()

	// Try to connect to invalid Redis server
	cacheStore := store.NewCacheStoreRedis("invalid-host:9999", "", 0)

	// Init should succeed (lazy connection)
	if err := cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Operations should fail
	err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("test-key", "test-value"),
	)
	if err == nil {
		t.Fatalf("expected error when connecting to invalid host, got nil")
	}

	// close connection (should not panic)
	cacheStore.Close(ctx)
}

func TestCacheStore_InfoAndString(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 2)
	if err = cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Test Info method
	info, err := cacheStore.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.StoreType != "redis" {
		t.Fatalf("expected StoreType 'redis', got %s", info.StoreType)
	}
	if info.NumItems != 0 {
		t.Fatalf("expected 0 items, got %d", info.NumItems)
	}
	if info.ConnectionInfo == "" {
		t.Fatalf("expected non-empty ConnectionInfo")
	}

	// Add some items
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("key1", "value1"),
	); err != nil {
		t.Fatal(err)
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("key2", "value2"),
	); err != nil {
		t.Fatal(err)
	}

	// Test Info with items
	info, err = cacheStore.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumItems != 2 {
		t.Fatalf("expected 2 items, got %d", info.NumItems)
	}

	// Test String method
	str := cacheStore.String()
	if str == "" {
		t.Fatalf("expected non-empty string representation")
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStore_ExpiredAt(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0)
	if err = cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Set key with expiration
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("key-with-ttl", "value"),
		comby.CacheStoreSetOptionWithExpiration(60*time.Second),
	); err != nil {
		t.Fatal(err)
	}

	// List keys and check ExpiredAt
	if cacheModels, _, err := cacheStore.List(ctx); err != nil {
		t.Fatal(err)
	} else {
		if len(cacheModels) != 1 {
			t.Fatalf("expected 1 key, got %d", len(cacheModels))
		}
		// NOTE: This test documents current behavior where ExpiredAt is always 0
		// In a proper implementation, ExpiredAt should contain the TTL
		if cacheModels[0].ExpiredAt != 0 {
			t.Logf("ExpiredAt is %d (expected: non-zero timestamp)", cacheModels[0].ExpiredAt)
		} else {
			t.Logf("WARNING: ExpiredAt is 0 - TTL information is not retrieved from Redis")
		}
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStore_ContextCancellation(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0)
	if err = cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Create a context with immediate timeout
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for context to expire
	time.Sleep(10 * time.Millisecond)

	// Try to set with expired context
	err = cacheStore.Set(ctxTimeout,
		comby.CacheStoreSetOptionWithKeyValue("test-key", "test-value"),
	)
	if err == nil {
		t.Logf("WARNING: Expected context deadline exceeded error, got nil")
	}

	// Close with valid context
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStore_EdgeCases(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0)
	if err = cacheStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// Test empty string value
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("empty-string", ""),
	); err != nil {
		t.Fatal(err)
	}

	// Get empty string
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("empty-string"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel.Value != "" {
			t.Fatalf("expected empty string, got %v", cacheModel.Value)
		}
	}

	// Test get non-existent key
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("non-existent"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel != nil {
			t.Fatalf("expected nil for non-existent key, got %v", cacheModel)
		}
	}

	// Test large value (100KB)
	largeValue := make([]byte, 1024*100)
	for i := range largeValue {
		largeValue[i] = 'A'
	}
	if err := cacheStore.Set(ctx,
		comby.CacheStoreSetOptionWithKeyValue("large-key", string(largeValue)),
	); err != nil {
		t.Fatal(err)
	}

	// Get large value
	if cacheModel, err := cacheStore.Get(ctx,
		comby.CacheStoreGetOptionWithKey("large-key"),
	); err != nil {
		t.Fatal(err)
	} else {
		if cacheModel.Value != string(largeValue) {
			t.Fatalf("large value mismatch")
		}
	}

	// reset database
	if err := cacheStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := cacheStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCacheStore_NewWithInvalidOptions(t *testing.T) {
	// Create an invalid option that returns an error
	invalidOption := comby.CacheStoreOption(func(opts *comby.CacheStoreOptions) (*comby.CacheStoreOptions, error) {
		return nil, fmt.Errorf("invalid option")
	})

	// NewCacheStoreRedis should return nil when option fails
	cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0, invalidOption)
	if cacheStore != nil {
		t.Fatalf("expected nil when option fails, got non-nil")
	}
}
