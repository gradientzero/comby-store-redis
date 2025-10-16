package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gradientzero/comby/v2"
	"github.com/redis/go-redis/v9"
)

type cacheStoreRedis struct {
	options      comby.CacheStoreOptions
	redisClient  *redis.Client
	redisOptions *redis.Options
}

// Make sure it implements interfaces
var _ comby.CacheStore = (*cacheStoreRedis)(nil)

func NewCacheStoreRedis(
	Addr string,
	Password string,
	DB int,
	opts ...comby.CacheStoreOption,
) comby.CacheStore {
	csr := &cacheStoreRedis{
		options: comby.CacheStoreOptions{},
		redisOptions: &redis.Options{
			Addr:     Addr,
			Password: Password,
			DB:       DB,
		},
	}
	for _, opt := range opts {
		if _, err := opt(&csr.options); err != nil {
			return nil
		}
	}
	return csr
}

// fullfilling CacheStore interface
func (csr *cacheStoreRedis) Init(ctx context.Context, opts ...comby.CacheStoreOption) error {
	for _, opt := range opts {
		if _, err := opt(&csr.options); err != nil {
			return err
		}
	}
	csr.redisClient = redis.NewClient(csr.redisOptions)
	return nil
}

func (csr *cacheStoreRedis) Get(ctx context.Context, opts ...comby.CacheStoreGetOption) (*comby.CacheModel, error) {
	getOpts := comby.CacheStoreGetOptions{}
	for _, opt := range opts {
		if _, err := opt(&getOpts); err != nil {
			return nil, err
		}
	}
	value, err := csr.redisClient.Get(ctx, getOpts.Key).Result()
	switch {
	case err == redis.Nil: // key does not exist
		return nil, nil
	case err != nil: // failed to get
		return nil, err
	}

	valueToReturn := any(value)

	// decrypt value if crypto service is provided
	if csr.options.CryptoService != nil {
		// value is stored as string in Redis, convert to []byte for decryption
		if strValue, ok := valueToReturn.(string); ok {
			decryptedValue, err := csr.decryptValue([]byte(strValue))
			if err != nil {
				return nil, err
			}
			valueToReturn = decryptedValue
		}
	}

	return &comby.CacheModel{
		Key:   getOpts.Key,
		Value: valueToReturn,
	}, nil
}

func (csr *cacheStoreRedis) Set(ctx context.Context, opts ...comby.CacheStoreSetOption) error {
	setOpts := comby.CacheStoreSetOptions{
		Expiration: 60 * time.Second,
	}
	for _, opt := range opts {
		if _, err := opt(&setOpts); err != nil {
			return err
		}
	}

	valueToStore := setOpts.Value

	// encrypt value if crypto service is provided
	if csr.options.CryptoService != nil {
		encryptedValue, err := csr.encryptValue(setOpts.Value)
		if err != nil {
			return err
		}
		valueToStore = encryptedValue
	}

	return csr.redisClient.Set(ctx, setOpts.Key, valueToStore, setOpts.Expiration).Err()
}

func (csr *cacheStoreRedis) List(ctx context.Context, opts ...comby.CacheStoreListOption) ([]*comby.CacheModel, int64, error) {
	listOpts := comby.CacheStoreListOptions{}
	for _, opt := range opts {
		if _, err := opt(&listOpts); err != nil {
			return nil, 0, err
		}
	}
	var items []*comby.CacheModel
	// TODO: naive implementation, should be replaced with SCAN
	keys, err := csr.redisClient.Keys(ctx, "*").Result()
	switch {
	case err == redis.Nil: // key does not exist
		return nil, 0, nil
	case err != nil: // failed to get
		return nil, 0, err
	}

	for _, key := range keys {
		value, err := csr.redisClient.Get(ctx, key).Result()
		if err != nil {
			return nil, 0, err
		}
		valid := len(listOpts.TenantUuid) == 0
		if !valid {
			// convention: prefix of key is the tenantUuid "%s-%s"
			valid = strings.HasPrefix(key, listOpts.TenantUuid)
		}
		if valid {
			valueToReturn := any(value)

			// decrypt value if crypto service is provided
			if csr.options.CryptoService != nil {
				// value is stored as string in Redis, convert to []byte for decryption
				if strValue, ok := valueToReturn.(string); ok {
					decryptedValue, err := csr.decryptValue([]byte(strValue))
					if err != nil {
						// skip items that fail to decrypt
						continue
					}
					valueToReturn = decryptedValue
				}
			}

			items = append(items, &comby.CacheModel{
				Key:       key,
				Value:     valueToReturn,
				ExpiredAt: 0,
			})
		}
	}
	var total int64 = int64(len(items))
	return items, total, nil
}

func (csr *cacheStoreRedis) Delete(ctx context.Context, opts ...comby.CacheStoreDeleteOption) error {
	deleteOpts := comby.CacheStoreDeleteOptions{}
	for _, opt := range opts {
		if _, err := opt(&deleteOpts); err != nil {
			return err
		}
	}
	if csr.redisClient != nil {
		ctx := context.Background()
		csr.redisClient.Del(ctx, deleteOpts.Key)
	}
	return nil
}

func (csr *cacheStoreRedis) Total(ctx context.Context) int64 {
	total := int64(0)
	if csr.redisClient != nil {
		return csr.redisClient.DBSize(ctx).Val()
	}
	return total
}

func (csr *cacheStoreRedis) Close(ctx context.Context) error {
	if csr.redisClient != nil {
		return csr.redisClient.Close()
	}
	return nil
}

func (csr *cacheStoreRedis) Options() comby.CacheStoreOptions {
	return csr.options
}

func (csr *cacheStoreRedis) String() string {
	return fmt.Sprintf("redis://%s:***@%s/%q", csr.redisOptions.Username, csr.redisOptions.Addr, csr.redisOptions.DB)
}

func (csr *cacheStoreRedis) Info(ctx context.Context) (*comby.CacheStoreInfoModel, error) {
	// total records
	dbTotal := int64(0)
	if csr.redisClient != nil {
		dbTotal = csr.redisClient.DBSize(ctx).Val()
	}

	return &comby.CacheStoreInfoModel{
		StoreType:      "redis",
		NumItems:       dbTotal,
		ConnectionInfo: fmt.Sprintf("redis://%s:***@%s/%q", csr.redisOptions.Username, csr.redisOptions.Addr, csr.redisOptions.DB),
	}, nil
}

func (csr *cacheStoreRedis) Reset(ctx context.Context) error {
	return csr.redisClient.FlushDB(ctx).Err()
}

func (csr *cacheStoreRedis) encryptValue(value any) ([]byte, error) {
	if csr.options.CryptoService == nil {
		return nil, fmt.Errorf("'%s' failed - crypto service is nil", csr.String())
	}
	// serialize value to JSON
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("'%s' failed - failed to marshal value: %w", csr.String(), err)
	}
	if len(valueBytes) < 1 {
		return nil, fmt.Errorf("'%s' failed - value is empty", csr.String())
	}
	// encrypt serialized value
	encryptedValue, err := csr.options.CryptoService.Encrypt(valueBytes)
	if err != nil {
		return nil, fmt.Errorf("'%s' failed - failed to encrypt value: %w", csr.String(), err)
	}
	return encryptedValue, nil
}

func (csr *cacheStoreRedis) decryptValue(encryptedValue []byte) (any, error) {
	if csr.options.CryptoService == nil {
		return nil, fmt.Errorf("'%s' failed - crypto service is nil", csr.String())
	}
	if len(encryptedValue) < 1 {
		return nil, fmt.Errorf("'%s' failed - encrypted value is empty", csr.String())
	}
	// decrypt value
	decryptedBytes, err := csr.options.CryptoService.Decrypt(encryptedValue)
	if err != nil {
		return nil, fmt.Errorf("'%s' failed - failed to decrypt value: %w", csr.String(), err)
	}
	// deserialize JSON to any
	var value any
	if err := json.Unmarshal(decryptedBytes, &value); err != nil {
		return nil, fmt.Errorf("'%s' failed - failed to unmarshal value: %w", csr.String(), err)
	}
	return value, nil
}
