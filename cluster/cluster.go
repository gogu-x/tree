// Package cluster 提供基于 etcd 的服务注册与发现功能，供 gate、game 等进程共用。
package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/gogu-x/tree/tlog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultTTL = int64(30) // 服务节点注册租约的有效秒数。
	// registrationRetryInterval 是服务节点重新注册的等待间隔。
	registrationRetryInterval = 2 * time.Second
)

func registerNode(client *clientv3.Client, key, addr string) (err error) {
	if client == nil {
		return fmt.Errorf("cluster: client is not initialized")
	}
	ctx := client.Ctx()
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	lease, err := client.Grant(requestCtx, defaultTTL)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = client.Revoke(requestCtx, lease.ID)
		}
	}()
	if _, err = client.Put(requestCtx, key, addr, clientv3.WithLease(lease.ID)); err != nil {
		return err
	}
	ch, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	tlog.Info("[cluster/registerNode] 节点已注册, key=%v addr=%v", key, addr)
	go func() {
		for range ch {
		}
		if ctx.Err() != nil {
			return
		}
		tlog.Error("[cluster/registerNode] 节点续租中断，将重试, key=%v addr=%v", key, addr)
		retryRegistration(client, key, addr)
	}()
	return nil
}

func retryRegistration(client *clientv3.Client, key, addr string) {
	ctx := client.Ctx()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-time.After(registrationRetryInterval):
		}
		if err := registerNode(client, key, addr); err != nil {
			if ctx.Err() != nil {
				return
			}
			tlog.Error("[cluster/retryRegistration] 节点重新注册失败, key=%v addr=%v err=%v", key, addr, err)
			continue
		}
		return
	}
}
