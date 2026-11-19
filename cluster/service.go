package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gogu-x/tree/tlog"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const servicePrefix = "/services/"

// ServiceInstance 是 /services/{type}/{serverID}/{nodeID} 下登记的服务节点。
type ServiceInstance struct {
	ServerID string
	NodeID   string
	Addr     string
}

// StartService 使用 Init 创建的客户端监听区服时间偏移并注册服务节点。
// 首次注册失败时后台重试。
func StartService(serviceType, serverID, nodeID, addr string) error {
	if err := validateServiceIdentity(serviceType, serverID, nodeID); err != nil {
		return err
	}
	if err := StartWithTimeOffset(serverID); err != nil {
		return err
	}
	key := serviceInstKey(serviceType, serverID, nodeID)
	client := etcdClient
	if err := registerNode(client, key, addr); err != nil {
		tlog.Error("[cluster/StartService] 节点注册失败，将重试, key=%v addr=%v err=%v", key, addr, err)
		go retryRegistration(client, key, addr)
	}
	return nil
}

// getServices 查询指定服务类型的节点，serverID 为空时查询该类型下的全部节点。
func getServices(serviceType, serverID string) ([]ServiceInstance, error) {
	if err := validateServiceSegment("type", serviceType); err != nil {
		return nil, err
	}
	if serverID != "" {
		if err := validateServiceSegment("serverID", serverID); err != nil {
			return nil, err
		}
	}
	client := etcdClient
	if client == nil {
		return nil, fmt.Errorf("cluster: client is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prefix := serviceTypePrefix(serviceType)
	if serverID != "" {
		prefix += serverID + "/"
	}
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	return serviceInstances(resp.Kvs), nil
}

// GetAllServices 返回指定服务类型的全部已登记节点。
func GetAllServices(serviceType string) ([]ServiceInstance, error) {
	return getServices(serviceType, "")
}

// GetServiceAddr 返回指定服务类型及区服下最新注册的节点地址。
func GetServiceAddr(serviceType, serverID string) (string, error) {
	if serverID == "" {
		return "", fmt.Errorf("cluster: serverID must not be empty")
	}
	instances, err := getServices(serviceType, serverID)
	if err != nil {
		return "", err
	}
	if len(instances) == 0 {
		return "", fmt.Errorf("cluster: service %s/%s not found", serviceType, serverID)
	}
	return instances[0].Addr, nil
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
	return ServiceInstance{ServerID: parts[1], NodeID: parts[2], Addr: string(kv.Value)}, true
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
