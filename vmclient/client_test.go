// package vmclient

// import (
// 	"context"
// 	"database/sql"
// 	"db-cmp/mongo_ops"
// 	"db-cmp/postgres"
// 	"encoding/json"
// 	"io/ioutil"
// 	"log"
// 	"testing"
// 	"time"

// 	"go.mongodb.org/mongo-driver/bson"
// 	"go.mongodb.org/mongo-driver/mongo"
// 	"go.mongodb.org/mongo-driver/mongo/options"
// )

// func TestClient(t *testing.T) {
// 	// f, err := os.Create("cpu.pprof")
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// }
// 	// pprof.StartCPUProfile(f)
// 	// defer pprof.StopCPUProfile()
// 	// vmc, err := NewVMClient("http://localhost:8428")
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// }
// 	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
// 	// defer cancel()
// 	// queryParams := map[string]string{
// 	// 	"match[]": "{__name__=~\".*\"}",
// 	// 	"start":   "-100d",
// 	// }
// 	// res, err := vmc.Series(ctx, queryParams)
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// }
// 	db, err := sql.Open("postgres", "postgres://postgres:password@localhost:7432/l9buffer?sslmode=disable")
// 	if err != nil {
// 		t.Error("aborting startup due to error", err.Error())
// 	}

// 	clientOpts := options.Client().ApplyURI(
// 		"mongodb://localhost:27017/?connect=direct")
// 	client, err := mongo.Connect(context.TODO(), clientOpts)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	// _, err = db.Exec("TRUNCATE TABLE tenant_labelset")
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// }
// 	s1 := time.Now()
// 	res, err := ioutil.ReadFile("/home/last9/series-final.json")
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	var s Series
// 	err = json.Unmarshal(res, &s)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	var values []interface{}
// 	var models []mongo.WriteModel

// 	for _, series := range s {
// 		var rec LabelRecord
// 		tenant, ok := series["__tenant__"]
// 		if !ok {
// 			continue
// 		}
// 		for k, v := range series {
// 			rec.labels = append(rec.labels, Label{
// 				Name:  k,
// 				Value: v,
// 			})
// 		}
// 		rec.tenant = tenant
// 		rec.hash = CardinalityHash(rec.labels)
// 		delete(series, "__tenant__")
// 		delete(series, "le")
// 		delete(series, "__l9lake__")
// 		b, err := json.Marshal(series)
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		bs := make(bson.M)

// 		for k, v := range series {
// 			bs[k] = v
// 		}

// 		mval := bson.D{
// 			{"$set", bson.D{
// 				{"updated_at", time.Now()},
// 			}},
// 			{"$setOnInsert", bson.D{
// 				{"_id", rec.hash},
// 				{"created_at", time.Now()},
// 				{"labels", bs},
// 			}},
// 		}
// 		models = append(models, mongo.NewUpdateOneModel().SetFilter(bson.D{{"_id", rec.hash}}).
// 			SetUpdate(mval).SetUpsert(true))
// 		values = append(values, rec.tenant, b, rec.hash, time.Now().Unix(), time.Now().Unix())
// 	}
// 	t.Log("process:", time.Since(s1))
// 	start := time.Now()
// 	err = postgres.BulkUpsert(context.Background(), db, "tenant_labelset", []string{"tenant", "labels", "cardinality", "created_at", "updated_at"}, values, []string{"tenant", "cardinality"}, []string{"tenant", "cardinality", "labels", "created_at"})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	t.Log("took:", time.Since(start))

// 	start1 := time.Now()
// 	err = mongo_ops.BulkUpsert(client, "last9", models)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	t.Log("took:", time.Since(start1))
// }
