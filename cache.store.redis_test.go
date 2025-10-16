package store_test

import (
	"context"
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
