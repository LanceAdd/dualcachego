package dualcache

import (
	"context"
	"errors"
	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcache"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/text/gstr"
	"sync"
	"time"
)

type DualCache struct {
	once     sync.Once
	topic    string
	l1Cache  *gcache.Cache
	l2Cache  *gredis.Redis
	shutdown chan bool
	wg       sync.WaitGroup
}

type CacheValue struct {
	Deadline time.Time
	Value    interface{}
}

func New(topic string, l1Cache *gcache.Cache, l2Cache *gredis.Redis) *DualCache {
	return &DualCache{topic: gstr.Trim(topic), l1Cache: l1Cache, l2Cache: l2Cache, shutdown: make(chan bool)}
}

func (d *DualCache) ShutDown() {
	d.shutdown <- true
	d.wg.Wait()
	close(d.shutdown)
}

func (d *DualCache) GetTopic() string {
	return d.topic
}

func (d *DualCache) Subscribe() {
	d.once.Do(func() {
		ctx := gctx.New()
		g.Log().Info(ctx, "DualCacheGo prepare subscribe topic:"+d.topic)
		go func(ctx context.Context) {
			defer d.wg.Done()
			conn, err := d.l2Cache.Conn(ctx)
			defer conn.Close(ctx)
			if err != nil {
				g.Log().Error(ctx, "Failed to connect to Redis:", err)
				return
			}
			_, err = conn.Subscribe(ctx, d.topic)
			if err != nil {

				g.Log().Error(ctx, "Failed to subscribe to topic:", err)
				return
			}
			g.Log().Info(ctx, "DualCacheGo start subscribe topic:"+d.topic)
			for {

				select {
				case <-d.shutdown:
					g.Log().Info(ctx, "DualCacheGo stop subscribe topic:"+d.topic)
					return
				default:
					message, err := conn.ReceiveMessage(ctx)
					if err != nil {
						g.Log().Error(ctx, err)
						continue
					}
					if gstr.Trim(message.Payload) != "" {
						_, err := d.l1Cache.Remove(ctx, message)
						if err != nil {
							g.Log().Error(ctx, err)
						}
					}
				}

			}
		}(ctx)
	})
}

func (d *DualCache) Set(ctx context.Context, key string, value interface{}, ttlInSeconds int64) (bool, error) {
	if ttlInSeconds <= 0 {
		return false, errors.New("ttl is empty")
	}
	err := d.l2Cache.SetEX(ctx, d.KeyGen(key), &CacheValue{Deadline: time.Now().Add(time.Duration(ttlInSeconds) * time.Second), Value: value}, ttlInSeconds)
	if err != nil {
		g.Log().Error(ctx, "Failed to set value in L2 cache:", err)
		return false, err
	}
	_, err = d.l2Cache.Publish(ctx, d.topic, key)
	if err != nil {
		g.Log().Error(ctx, "Failed to publish message to topic:", err)
		return false, err
	}
	return true, nil
}

func (d *DualCache) Get(ctx context.Context, key string) (any, error) {
	cacheKey := d.KeyGen(key)
	l1, err := d.l1Cache.Get(ctx, cacheKey)
	if err != nil {
		g.Log().Errorf(ctx, "Failed to get value from L1 cache with topic:%s key: %s\n%+v", d.topic, key, err)
		return nil, err
	}
	if l1.IsNil() {
		l2, err := d.l2Cache.Get(ctx, cacheKey)
		if err != nil {
			g.Log().Errorf(ctx, "Failed to get value from L2 cache with topic:%s key: %s\n%+v", d.topic, key, err)
			return nil, err
		}
		if l2.IsNil() {
			return nil, nil
		}
		var value CacheValue
		err = l2.Scan(&value)
		if err != nil {
			g.Log().Errorf(ctx, "Failed to scan value from l2 CacheValue with topic:%s key: %s\n%+v", d.topic, key, err)

		}
		sub := time.Now().Sub(value.Deadline)
		if (sub - 10*time.Second) < 10*time.Second {
			return gvar.New(value.Value), nil
		}
		err = d.l1Cache.Set(ctx, cacheKey, l2.Val(), sub-10*time.Second)
		if err != nil {
			g.Log().Errorf(ctx, "Failed to set value in L1 cache with topic:%s key: %s\n%+v", d.topic, key, err)
			return nil, err
		}
		return gvar.New(value.Value), nil
	}
	return l1, err
}

func (d *DualCache) KeyGen(key string) string {
	return d.topic + ":" + key
}
