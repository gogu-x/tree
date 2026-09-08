package cluster

import (
	"context"
	"reflect"
	"testing"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

func TestServiceKeyAndParsing(t *testing.T) {
	key := serviceInstKey("game", "1001", "3")
	if key != "/services/game/1001/3" {
		t.Fatalf("service key = %q", key)
	}
	inst, ok := parseServiceKV(&mvccpb.KeyValue{Key: []byte(key), Value: []byte("127.0.0.1:9000")})
	want := ServiceInstance{Type: "game", ServiceType: "game", ServerID: "1001", NodeID: "3", Addr: "127.0.0.1:9000"}
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

func TestGenericServiceAPIsAreSafeBeforeInit(t *testing.T) {
	oldClient := Client
	Client = nil
	t.Cleanup(func() { Client = oldClient })

	if err := RegisterService("game", "1", "1", "addr"); err == nil {
		t.Fatal("RegisterService succeeded without Init")
	}
	if err := DeregisterService("game", "1", "1"); err == nil {
		t.Fatal("DeregisterService succeeded without Init")
	}
	if _, err := GetServices("game", "1"); err == nil {
		t.Fatal("GetServices succeeded without Init")
	}
	ch := WatchServices(context.Background(), "game")
	if _, ok := <-ch; ok {
		t.Fatal("WatchServices channel remained open without Init")
	}
}
