package cluster

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gogu-x/tree/comm"
	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type timeOffsetKV struct {
	clientv3.KV
	get func(string) *clientv3.GetResponse
}

func (kv timeOffsetKV) Get(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return kv.get(key), nil
}

type timeOffsetWatcher struct {
	clientv3.Watcher
	watch func(string, int64) clientv3.WatchChan
	close func()
}

func (w timeOffsetWatcher) Watch(_ context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return w.watch(key, clientv3.OpGet(key, opts...).Rev())
}

func (w timeOffsetWatcher) Close() error {
	w.close()
	return nil
}

func TestWatchTimeOffsetLoadsAndAppliesChanges(t *testing.T) {
	oldClient := etcdClient
	t.Cleanup(func() {
		etcdClient = oldClient
		_, _ = comm.SetTimeOffset(0)
	})
	synctest.Test(t, func(t *testing.T) {
		key := "/time/101"
		initial := &clientv3.GetResponse{Header: &etcdpb.ResponseHeader{Revision: 10}, Kvs: []*mvccpb.KeyValue{{Value: []byte("3600")}}}
		missing := &clientv3.GetResponse{Header: &etcdpb.ResponseHeader{Revision: 20}}
		first := make(chan clientv3.WatchResponse, 1)
		second := make(chan clientv3.WatchResponse, 1)
		gets := 0
		client := clientv3.NewCtxClient(t.Context())
		client.KV = timeOffsetKV{get: func(gotKey string) *clientv3.GetResponse {
			if gotKey != key {
				t.Errorf("Get key = %q, want %q", gotKey, key)
			}
			gets++
			if gets == 1 {
				return initial
			}
			return missing
		}}
		client.Watcher = timeOffsetWatcher{
			watch: func(gotKey string, rev int64) clientv3.WatchChan {
				if gotKey != key || (rev != 11 && rev != 21) {
					t.Errorf("Watch key = %q, revision = %d", gotKey, rev)
				}
				if rev == 11 {
					return first
				}
				return second
			},
			close: func() {
				close(first)
				close(second)
			},
		}
		etcdClient = client
		defer Close()
		checkOffset := func(want time.Duration) {
			t.Helper()
			if got := comm.Now().Sub(comm.RealNow()); got != want {
				t.Errorf("game time offset = %s, want %s", got, want)
			}
		}
		if err := StartWithTimeOffset("101"); err != nil {
			t.Fatal(err)
		}
		checkOffset(time.Hour)
		first <- clientv3.WatchResponse{Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Value: []byte("-1800")}}}}
		synctest.Wait()
		checkOffset(-30 * time.Minute)
		first <- clientv3.WatchResponse{Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Value: []byte("bad")}}}}
		synctest.Wait()
		checkOffset(-30 * time.Minute)
		first <- clientv3.WatchResponse{CompactRevision: 20}
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		checkOffset(0)
		second <- clientv3.WatchResponse{Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Value: []byte("60")}}}}
		synctest.Wait()
		checkOffset(time.Minute)
		second <- clientv3.WatchResponse{Events: []*clientv3.Event{{Type: clientv3.EventTypeDelete}}}
		synctest.Wait()
		checkOffset(0)
	})
}
