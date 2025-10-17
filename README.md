# comby-store-redis

Simple implementation of the `CacheStore` interface defined in [comby](https://github.com/gradientzero/comby) with Redis. **comby** is a powerful application framework designed with Event Sourcing and Command Query Responsibility Segregation (CQRS) principles, written in Go.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## Prerequisites

- [Golang 1.22+](https://go.dev/dl/)
- [comby](https://github.com/gradientzero/comby)
- [Redis-Server](https://redis.io/downloads/)

```shell
# run redis server locally for testings
docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest
```

## Installation

*comby-store-redis* supports the latest version of comby (v2), requires Go version 1.22+ and is based on Redis client v9.0.0.

```shell
go get github.com/gradientzero/comby-store-redis
```

## Quickstart

```go
import (
	"github.com/gradientzero/comby-store-redis"
	"github.com/gradientzero/comby/v2"
)

// create redis CacheStore
cacheStore := store.NewCacheStoreRedis("localhost:6379", "", 0)
if err = cacheStore.Init(ctx,
    comby.CacheStoreOptionWithAttribute("anyKey", "anyValue"),
); err != nil {
    panic(err)
}

// create Facade
fc, _ := comby.NewFacade(
  comby.FacadeWithCacheStore(cacheStore),
)
```

## Tests

```bash
go fmt ./...
go clean -testcache
go test -v ./... -covermode=count
go test -v ./... -race
go vet ./...

# go install honnef.co/go/tools/cmd/staticcheck@latest
staticcheck ./...
```

## Contributing
Please follow the guidelines in [CONTRIBUTING.md](./CONTRIBUTING.md).

## License
This project is licensed under the [MIT License](./LICENSE.md).

## Contact
https://www.gradient0.com
