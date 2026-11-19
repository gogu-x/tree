package cluster

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var etcdClient *clientv3.Client

// Init 根据启动配置初始化进程共用的 etcd 客户端。
func Init(ctx context.Context, endpoints []string) error {
	client, err := clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("初始化 etcd 失败: %w", err)
	}
	etcdClient = client
	return nil
}

// Close 关闭 etcd 客户端，并停止使用该客户端上下文的监听和续租。
func Close() {
	if client := etcdClient; client != nil {
		_ = client.Close()
	}
}
