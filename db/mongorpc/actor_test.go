package mongorpc

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gogu-x/tree"
	"github.com/gogu-x/tree/tlog"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestCreateIndexes 需要本地 Mongo：MONGO_TEST_URI=mongodb://127.0.0.1:27017 go test ./db/mongorpc/
func TestCreateIndexes(t *testing.T) {
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Skip("MONGO_TEST_URI not set")
	}
	logger, err := tlog.New("error", "", tlog.NoFlags)
	if err != nil {
		t.Fatal(err)
	}
	tlog.Log = logger

	dbName := fmt.Sprintf("tree_index_test_%d", time.Now().UnixNano())
	db := Connect(uri, "", "", dbName)
	defer func() { _ = db.Drop(context.Background()) }()

	system := tree.NewTree()
	defer system.Shutdown()
	pid := system.SpawnOne(NewActor("mongo_test", db))

	request := &CreateIndexes{
		Collection: "docs",
		Indexes: []mongo.IndexModel{
			{Keys: bson.D{{Key: "owner_scope", Value: 1}, {Key: "owner_id", Value: 1}}, Options: options.Index().SetName("idx_owner")},
		},
	}
	// 连续两次创建：第二次定义相同，应幂等成功。
	for round := 0; round < 2; round++ {
		value, err := system.Request(pid, request).AwaitTimeout(10 * time.Second)
		if err != nil {
			t.Fatalf("round %d create indexes: %v", round, err)
		}
		if names, ok := value.([]string); !ok || len(names) != 1 || names[0] != "idx_owner" {
			t.Fatalf("round %d unexpected result %#v", round, value)
		}
	}

	cursor, err := db.Collection("docs").Indexes().List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var specs []bson.M
	if err := cursor.All(context.Background(), &specs); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, spec := range specs {
		if spec["name"] == "idx_owner" {
			found = true
		}
	}
	if !found {
		t.Fatalf("index not created, got %v", specs)
	}

	// 同名不同定义应返回错误。
	conflict := &CreateIndexes{
		Collection: "docs",
		Indexes:    []mongo.IndexModel{{Keys: bson.D{{Key: "owner_id", Value: 1}}, Options: options.Index().SetName("idx_owner")}},
	}
	if _, err := system.Request(pid, conflict).AwaitTimeout(10 * time.Second); err == nil {
		t.Fatal("conflicting index definition should fail")
	}
}
