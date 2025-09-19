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
