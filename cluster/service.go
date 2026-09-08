package cluster

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const servicePrefix = "/services/"

// ServiceInstance describes one entry under
// /services/{type}/{serverID}/{nodeID}.
type ServiceInstance struct {
	// Type is kept as the concise service-type name; ServiceType is an explicit
	// alias useful when ServiceInstance is embedded in ServiceEvent.
	Type        string
	ServiceType string
	ServerID    string
	NodeID      string
	Addr        string
}

// ServiceEvent describes a service registry change. Type and Event both hold
// "put" or "delete"; Type mirrors the legacy InstanceEvent convention.
type ServiceEvent struct {
	ServiceInstance
	Type  string
	Event string
}

// RegisterService registers a generic service node using the same default TTL
// and keepalive behavior as the legacy Register API.
func RegisterService(serviceType, serverID, nodeID, addr string) error {
	if err := validateServiceIdentity(serviceType, serverID, nodeID); err != nil {
		return err
	}
	return registerServiceWithTTL(serviceType, serverID, nodeID, addr, defaultTTL)
}

func registerServiceWithTTL(serviceType, serverID, nodeID, addr string, ttl int64) error {
	if Client == nil {
		return fmt.Errorf("cluster: client is not initialized")
	}
	lease, err := Client.Grant(context.Background(), ttl)
	if err != nil {
		return err
	}
	key := serviceInstKey(serviceType, serverID, nodeID)
	if _, err = Client.Put(context.Background(), key, addr, clientv3.WithLease(lease.ID)); err != nil {
		_, _ = Client.Revoke(context.Background(), lease.ID)
		return err
	}
	ch, err := Client.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		_, _ = Client.Revoke(context.Background(), lease.ID)
		return err
	}
	go func() {
		for range ch {
		}
		log.Printf("cluster: service keepalive lost for [%s/%s/%s], re-registering...", serviceType, serverID, nodeID)
		for {
			time.Sleep(2 * time.Second)
			if err := registerServiceWithTTL(serviceType, serverID, nodeID, addr, ttl); err != nil {
				log.Printf("cluster: service re-register failed: %v", err)
				continue
			}
			return
		}
	}()
	return nil
}

// DeregisterService removes a generic service node immediately.
func DeregisterService(serviceType, serverID, nodeID string) error {
	if err := validateServiceIdentity(serviceType, serverID, nodeID); err != nil {
		return err
	}
	if Client == nil {
		return fmt.Errorf("cluster: client is not initialized")
	}
	_, err := Client.Delete(context.Background(), serviceInstKey(serviceType, serverID, nodeID))
	return err
}

// GetServices queries generic service entries. An empty serverID returns all
// servers of the requested type; serviceType must be a single non-empty path
// segment.
func GetServices(serviceType, serverID string) ([]ServiceInstance, error) {
	if err := validateServiceSegment("type", serviceType); err != nil {
		return nil, err
	}
	if serverID != "" {
		if err := validateServiceSegment("serverID", serverID); err != nil {
			return nil, err
		}
	}
	if Client == nil {
		return nil, fmt.Errorf("cluster: client is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prefix := serviceTypePrefix(serviceType)
	if serverID != "" {
		prefix += serverID + "/"
	}
	resp, err := Client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	return serviceInstances(resp.Kvs), nil
}

// GetServiceInstances is a named convenience wrapper for querying one server.
func GetServiceInstances(serviceType, serverID string) ([]ServiceInstance, error) {
	if serverID == "" {
		return nil, fmt.Errorf("cluster: serverID must not be empty")
	}
	return GetServices(serviceType, serverID)
}

// GetAllServices returns every server/node registered for one service type.
func GetAllServices(serviceType string) ([]ServiceInstance, error) {
	return GetServices(serviceType, "")
}

// GetServiceAddr returns the most recently registered node address for a
// service type/server pair.
func GetServiceAddr(serviceType, serverID string) (string, error) {
	instances, err := GetServiceInstances(serviceType, serverID)
	if err != nil {
		return "", err
	}
	if len(instances) == 0 {
		return "", fmt.Errorf("cluster: service %s/%s not found", serviceType, serverID)
	}
	return instances[0].Addr, nil
}

// WatchServices watches /services/{type}/. Passing an empty serviceType watches
// all generic service types. The returned channel closes when the etcd watch
// ends or ctx is cancelled.
func WatchServices(ctx context.Context, serviceType string) <-chan ServiceEvent {
	out := make(chan ServiceEvent, 64)
	if ctx == nil {
		ctx = context.Background()
	}
	if serviceType != "" {
		if err := validateServiceSegment("type", serviceType); err != nil {
			close(out)
			return out
		}
	}
	if Client == nil {
		close(out)
		return out
	}
	prefix := servicePrefix
	if serviceType != "" {
		prefix = serviceTypePrefix(serviceType)
	}
	go func() {
		defer close(out)
		wch := Client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for wresp := range wch {
			for _, ev := range wresp.Events {
				inst, ok := parseServiceKV(ev.Kv)
				if !ok {
					continue
				}
				eventType := "put"
				if ev.Type == clientv3.EventTypeDelete {
					eventType = "delete"
					if ev.PrevKv != nil {
						inst.Addr = string(ev.PrevKv.Value)
					}
				}
				select {
				case out <- ServiceEvent{ServiceInstance: inst, Type: eventType, Event: eventType}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func serviceInstances(kvs []*mvccpb.KeyValue) []ServiceInstance {
	ordered := append([]*mvccpb.KeyValue(nil), kvs...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].CreateRevision > ordered[j].CreateRevision
	})
	result := make([]ServiceInstance, 0, len(ordered))
	for _, kv := range ordered {
		if inst, ok := parseServiceKV(kv); ok {
			result = append(result, inst)
		}
	}
	return result
}

func parseServiceKV(kv *mvccpb.KeyValue) (ServiceInstance, bool) {
	if kv == nil {
		return ServiceInstance{}, false
	}
	relative := strings.TrimPrefix(string(kv.Key), servicePrefix)
	if relative == string(kv.Key) {
		return ServiceInstance{}, false
	}
	parts := strings.Split(relative, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return ServiceInstance{}, false
	}
	return ServiceInstance{Type: parts[0], ServiceType: parts[0], ServerID: parts[1], NodeID: parts[2], Addr: string(kv.Value)}, true
}

func serviceInstKey(serviceType, serverID, nodeID string) string {
	return serviceTypePrefix(serviceType) + serverID + "/" + nodeID
}

func serviceTypePrefix(serviceType string) string {
	return servicePrefix + serviceType + "/"
}

func validateServiceIdentity(serviceType, serverID, nodeID string) error {
	if err := validateServiceSegment("type", serviceType); err != nil {
		return err
	}
	if err := validateServiceSegment("serverID", serverID); err != nil {
		return err
	}
	return validateServiceSegment("nodeID", nodeID)
}

func validateServiceSegment(name, value string) error {
	if value == "" || strings.ContainsAny(value, "/\\") || value == "." || value == ".." {
		return fmt.Errorf("cluster: invalid service %s %q", name, value)
	}
	return nil
}
