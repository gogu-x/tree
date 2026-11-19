package cluster

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gogu-x/tree/comm"
	"github.com/gogu-x/tree/tlog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const timePrefix = "/time/" // /time/{serverID}，值为有符号的秒数偏移。

// StartWithTimeOffset 使用 Init 创建的客户端加载并监听区服时间偏移。
// 键不存在时使用零偏移。监听从初始快照的下一版本开始，避免漏掉并发写入。
func StartWithTimeOffset(serverID string) error {
	client := etcdClient
	if client == nil {
		return fmt.Errorf("cluster: client is not initialized")
	}
	if err := validateServiceSegment("serverID", serverID); err != nil {
		return err
	}
	ctx := client.Ctx()
	key := timePrefix + serverID
	value, revision, err := readTimeOffset(ctx, client, key)
	if err != nil {
		return err
	}
	if err := applyTimeOffset(key, value); err != nil {
		return err
	}
	go syncTimeOffset(ctx, client, key, revision)
	return nil
}

func readTimeOffset(ctx context.Context, client *clientv3.Client, key string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		return "", 0, fmt.Errorf("读取区服时间偏移 %s: %w", key, err)
	}
	revision := resp.Header.GetRevision()
	if len(resp.Kvs) == 0 {
		return "0", revision, nil
	}
	return string(resp.Kvs[0].GetValue()), revision, nil
}

func applyTimeOffset(key, value string) error {
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("区服时间偏移 %s 的值 %q 无效: %w", key, value, err)
	}
	before, err := comm.SetTimeOffset(seconds)
	if err != nil {
		return fmt.Errorf("区服时间偏移 %s: %w", key, err)
	}
	if before != seconds {
		tlog.Info("[cluster/applyTimeOffset] 区服时间偏移已更新, key=%v before=%v after=%v", key, before, seconds)
	}
	return nil
}

func updateTimeOffset(key, value string) {
	if err := applyTimeOffset(key, value); err != nil {
		tlog.Error("[cluster/updateTimeOffset] 区服时间偏移更新失败, err=%v", err)
	}
}

func watchOffsetChanges(ctx context.Context, client *clientv3.Client, key string, revision int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for resp := range client.Watch(ctx, key, clientv3.WithRev(revision+1)) {
		if err := resp.Err(); err != nil {
			return err
		}
		for _, event := range resp.Events {
			if err := ctx.Err(); err != nil {
				return err
			}
			value := "0"
			if event.GetType() != clientv3.EventTypeDelete {
				value = string(event.GetKv().GetValue())
			}
			updateTimeOffset(key, value)
		}
	}
	return ctx.Err()
}

func syncTimeOffset(ctx context.Context, client *clientv3.Client, key string, revision int64) {
	ctx = clientv3.WithRequireLeader(ctx)
	for ctx.Err() == nil {
		if err := watchOffsetChanges(ctx, client, key, revision); err != nil && ctx.Err() == nil {
			tlog.Error("[cluster/syncTimeOffset] 区服时间偏移监听中断, key=%v err=%v", key, err)
		}
		// 监听历史压缩或流终止后重新读取快照，保留旧偏移直到拿到有效新值。
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			value, latest, err := readTimeOffset(ctx, client, key)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				tlog.Error("[cluster/syncTimeOffset] 区服时间偏移重新读取失败, err=%v", err)
				continue
			}
			updateTimeOffset(key, value)
			revision = latest
			break
		}
	}
}
