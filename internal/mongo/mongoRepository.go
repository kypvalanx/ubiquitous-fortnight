package mongo

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Repository struct {
	collection *mongo.Collection
	client     *mongo.Client
}
type RepositoryConfig struct {
	MongoDbUri        string
	MongoDbDatabase   string
	MongoDbCollection string
}

type Document struct {
	ID            bson.ObjectID `bson:"_id,omitempty"`
	CorrelationID string        `bson:"correlationId"`
	Type          string        `bson:"type"`
	Payload       any           `bson:"payload"`
	Status        string        `bson:"status"`
	CreatedAt     time.Time     `bson:"createdAt"`
}

func NewRepository(config RepositoryConfig) *Repository {
	client, err := mongo.Connect(
		options.Client().ApplyURI(config.MongoDbUri),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := client.Database(
		config.MongoDbDatabase,
	)

	return &Repository{
		client:     client,
		collection: db.Collection(config.MongoDbCollection),
	}
}

func (r *Repository) Close(ctx context.Context) {
	err := r.client.Disconnect(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}
}

func (r *Repository) Write(message Document) any {
	one, err := r.collection.InsertOne(context.Background(), message)
	if err != nil {
		return nil
	}
	return one.InsertedID
}

func (r *Repository) Read(ctx context.Context, correlationID string) *Document {
	document := &Document{}
	err := r.collection.FindOne(ctx, bson.M{"correlationId": correlationID}).Decode(document)
	if err != nil {
		return nil
	}
	return document
}

func (r *Repository) ReadAll(ctx context.Context, status string) *[]Document {
	response, _ := r.collection.Find(ctx, bson.M{"status": status})

	var documents []Document
	err := response.All(ctx, &documents)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return &documents
}
