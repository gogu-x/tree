package cluster

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestServiceKeyAndParsing(t *testing.T) {
	key := serviceInstKey("game", "1001", "3")
	if key != "/services/game/1001/3" {
		t.Fatalf("service key = %q", key)
	}
	inst, ok := parseServiceKV(&mvccpb.KeyValue{Key: []byte(key), Value: []byte("127.0.0.1:9000")})
	want := ServiceInstance{ServerID: "1001", NodeID: "3", Addr: "127.0.0.1:9000"}
	if !ok || !reflect.DeepEqual(inst, want) {
		t.Fatalf("parsed = %#v, %v; want %#v", inst, ok, want)
	}

	invalid := []string{
		"/game/1001/3",
		"/services/game/1001",
		"/services/game/1001/3/extra",
		"/services//1001/3",
	}
	for _, candidate := range invalid {
		if _, ok := parseServiceKV(&mvccpb.KeyValue{Key: []byte(candidate)}); ok {
			t.Errorf("accepted invalid key %q", candidate)
		}
	}
}

type registrationEtcd struct {
	clientv3.KV
	clientv3.Lease
	grants      int
	writes      int
	key         string
	addr        string
	keepAliveCh chan *clientv3.LeaseKeepAliveResponse
}

func (m *registrationEtcd) Grant(_ context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	m.grants++
	if m.grants == 2 {
		return nil, errors.New("temporary lease failure")
	}
	return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(m.grants), TTL: ttl}, nil
}

func (m *registrationEtcd) Put(_ context.Context, key, addr string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	m.key, m.addr = key, addr
	m.writes++
	return &clientv3.PutResponse{}, nil
}

func (m *registrationEtcd) Get(_ context.Context, prefix string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if !strings.HasPrefix(m.key, prefix) {
		return &clientv3.GetResponse{}, nil
	}
	return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte(m.key), Value: []byte(m.addr)}}}, nil
}

func (m *registrationEtcd) KeepAlive(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	m.keepAliveCh = make(chan *clientv3.LeaseKeepAliveResponse)
	return m.keepAliveCh, nil
}

func (m *registrationEtcd) stopKeepAlive() {
	if m.keepAliveCh != nil {
		close(m.keepAliveCh)
		m.keepAliveCh = nil
	}
}

func TestRegisteredServiceIsDiscoverableAndRecoversLease(t *testing.T) {
	oldClient := etcdClient
	t.Cleanup(func() { etcdClient = oldClient })
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		mock := &registrationEtcd{}
		defer func() {
			cancel()
			mock.stopKeepAlive()
		}()
		client := clientv3.NewCtxClient(ctx)
		client.KV, client.Lease = mock, mock
		etcdClient = client

		if err := registerNode(client, serviceInstKey("Battle", "101", "1"), "battle:7201"); err != nil {
			t.Fatal(err)
		}
		if mock.key != "/services/Battle/101/1" {
			t.Fatalf("registration key = %q", mock.key)
		}
		if addr, err := GetServiceAddr("Battle", "101"); err != nil || addr != "battle:7201" {
			t.Fatalf("Battle address = %q, err = %v", addr, err)
		}
		if _, err := GetServiceAddr("Game", "101"); err == nil {
			t.Fatalf("missing Game address returned %v", err)
		}
		mock.stopKeepAlive()
		synctest.Wait()
		time.Sleep(2 * registrationRetryInterval)
		synctest.Wait()
		if mock.grants != 3 || mock.writes != 2 || mock.keepAliveCh == nil {
			t.Fatalf("after retry: grants = %d, writes = %d, keepalive = %v", mock.grants, mock.writes, mock.keepAliveCh)
		}
		cancel()
		mock.stopKeepAlive()
		time.Sleep(registrationRetryInterval)
		synctest.Wait()
		if mock.grants != 3 {
			t.Fatalf("registration retried after shutdown: grants = %d", mock.grants)
		}
	})
}

func TestServiceInstancesNewestFirst(t *testing.T) {
	kvs := []*mvccpb.KeyValue{
		{Key: []byte("/services/game/1/old"), Value: []byte("old"), CreateRevision: 3},
		{Key: []byte("/services/game/1/new"), Value: []byte("new"), CreateRevision: 9},
		{Key: []byte("/not-a-service"), CreateRevision: 20},
	}
	got := serviceInstances(kvs)
	if len(got) != 2 || got[0].NodeID != "new" || got[1].NodeID != "old" {
		t.Fatalf("instances = %#v", got)
	}
	// Sorting must not mutate the etcd response slice supplied by the caller.
	if string(kvs[0].Key) != "/services/game/1/old" {
		t.Fatalf("input slice was mutated")
	}
}

func TestServiceIdentityValidation(t *testing.T) {
	for _, tc := range []struct{ typ, server, node string }{
		{"", "1", "1"},
		{"game/other", "1", "1"},
		{"game", "", "1"},
		{"game", "../1", "1"},
		{"game", "1", "node/2"},
	} {
		if err := validateServiceIdentity(tc.typ, tc.server, tc.node); err == nil {
			t.Errorf("accepted invalid identity %#v", tc)
		}
	}
	if err := validateServiceIdentity("game", "1001", "3"); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
}

func TestGetAllServicesRequiresClient(t *testing.T) {
	oldClient := etcdClient
	etcdClient = nil
	t.Cleanup(func() { etcdClient = oldClient })

	if _, err := GetAllServices("Game"); err == nil {
		t.Fatal("GetAllServices succeeded without an etcd client")
	}
}
