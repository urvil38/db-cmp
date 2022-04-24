package mongo_ops

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func BulkUpsert(client *mongo.Client, collection string, models []mongo.WriteModel) error {
	coll := client.Database("l9buffer").Collection(collection)
	opts := options.BulkWrite().SetOrdered(false)
	res, err := coll.BulkWrite(context.TODO(), models, opts)
	if err != nil {
		return err
	}

	fmt.Printf(
		"inserted %v and upserted %v documents\n",
		res.InsertedCount,
		res.ModifiedCount)
	return nil
}
