package codec

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// 格式：| 2字节msgID | protobuf消息体 |

var ProtoCodec = &protoCodec{
	idToType: make(map[uint16]reflect.Type),
	typeToID: make(map[reflect.Type]uint16),
}

type protoCodec struct {
	idToType map[uint16]reflect.Type
	typeToID map[reflect.Type]uint16
}

func msgID(msg proto.Message) uint16 {
	t := reflect.TypeOf(msg).Elem()
	h := fnv.New32a()
	h.Write([]byte(t.PkgPath() + "." + t.Name()))
	return uint16(h.Sum32())
}

func (c *protoCodec) register(msg proto.Message) {
	t := reflect.TypeOf(msg)
	id := msgID(msg)
	c.idToType[id] = t
	c.typeToID[t] = id
}

func (c *protoCodec) Unmarshal(data []byte) (interface{}, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("proto: data too short")
	}
	id := binary.BigEndian.Uint16(data[:2])
	t, ok := c.idToType[id]
	if !ok {
		registered := make([]string, 0, len(c.idToType))
		for rid, rt := range c.idToType {
			registered = append(registered, fmt.Sprintf("%d=%s", rid, rt.Elem().Name()))
		}
		return nil, fmt.Errorf("proto: unknown msgID %d, registered: %v", id, registered)
	}
	msg := reflect.New(t.Elem()).Interface().(proto.Message)
	return msg, proto.Unmarshal(data[2:], msg)
}

func (c *protoCodec) Marshal(msg interface{}) ([]byte, error) {
	t := reflect.TypeOf(msg)
	id, ok := c.typeToID[t]
	if !ok {
		return nil, fmt.Errorf("proto: message %s not registered", t)
	}
	body, err := proto.Marshal(msg.(proto.Message))
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 2+len(body))
	binary.BigEndian.PutUint16(buf[:2], id)
	copy(buf[2:], body)
	return buf, nil
}

type ReqHeader struct {
	UID       uint64
	ServerID  uint32
	SessionID string
}

// WriteHeader 写入公共头（gate 转发前填充/覆盖，防客户端伪造）。
func WriteHeader(m proto.Message, h ReqHeader) {
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	if fd := fields.ByName("uID"); fd != nil && fd.Kind() == protoreflect.Uint64Kind {
		r.Set(fd, protoreflect.ValueOfUint64(h.UID))
	}
	if fd := fields.ByName("serverID"); fd != nil && fd.Kind() == protoreflect.Uint32Kind {
		r.Set(fd, protoreflect.ValueOfUint32(h.ServerID))
	}
	if fd := fields.ByName("sessionID"); fd != nil && fd.Kind() == protoreflect.StringKind {
		r.Set(fd, protoreflect.ValueOfString(h.SessionID))
	}
}
