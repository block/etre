package schema_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/square/etre/schema"
	"github.com/square/etre/test"
)

const testEntityType = "schema_test"

func TestCreateOrUpdateMongoSchema_Integration(t *testing.T) {
	// Setup: Connect to MongoDB
	client, colls, err := test.DbCollections([]string{testEntityType})
	require.NoError(t, err, "failed to connect to MongoDB")
	defer client.Disconnect(context.Background())

	db := client.Database("etre_test")
	coll := colls[testEntityType]

	// Clean up before and after test
	cleanup := func() {
		// Drop the collection to start fresh
		err := coll.Drop(context.Background())
		if err != nil {
			t.Logf("Warning: failed to drop collection: %v", err)
		}
	}
	cleanup()
	defer cleanup()

	t.Run("create indexes and validation", func(t *testing.T) {
		// Define a schema with indexes and field validations
		config := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "hostname", Type: "string", Required: true},
							{Name: "region", Type: "string", Required: true, Enum: []string{"us-east-1", "us-west-2"}},
							{Name: "port", Type: "int", Required: false},
							{Name: "active", Type: "bool"},
						},
						AdditionalProperties: true, // Must be true to allow MongoDB's auto-generated _id field
						ValidationLevel:      "strict",
						Indexes: []schema.Index{
							{
								Keys:   []string{"hostname"},
								Unique: true,
							},
							{
								Keys: []string{"region", "hostname"},
							},
						},
					},
				},
			},
		}

		// Apply the schema to MongoDB
		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, config)
		require.NoError(t, err, "failed to create or update schema")

		// Verify indexes were created
		indexes, err := getIndexes(context.Background(), coll)
		require.NoError(t, err, "failed to get indexes")

		// Should have: _id_ (default), IL_hostname, SL_region_hostname
		assert.Len(t, indexes, 3, "expected 3 indexes: _id_, IL_hostname, SL_region_hostname")
		assert.Contains(t, indexes, "_id_", "should have default _id_ index")
		assert.Contains(t, indexes, "IL_hostname", "should have unique hostname index")
		assert.Contains(t, indexes, "SL_region_hostname", "should have compound region_hostname index")

		// Verify validation rules by attempting to insert documents
		ctx := context.Background()

		// Valid document should succeed
		validDoc := bson.M{
			"hostname": "server01",
			"region":   "us-east-1",
			"port":     int64(8080),
			"active":   true,
		}
		_, err = coll.InsertOne(ctx, validDoc)
		assert.NoError(t, err, "valid document should be inserted")

		// Invalid document: missing required field
		invalidDoc1 := bson.M{
			"region": "us-west-2",
			"port":   int64(8080),
		}
		_, err = coll.InsertOne(ctx, invalidDoc1)
		assert.Error(t, err, "document missing required field should fail")

		// Invalid document: enum violation
		invalidDoc2 := bson.M{
			"hostname": "server02",
			"region":   "eu-west-1", // Not in enum
			"port":     int64(8080),
		}
		_, err = coll.InsertOne(ctx, invalidDoc2)
		assert.Error(t, err, "document with invalid enum value should fail")

		// Invalid document: duplicate unique key
		duplicateDoc := bson.M{
			"hostname": "server01", // Duplicate
			"region":   "us-west-2",
		}
		_, err = coll.InsertOne(ctx, duplicateDoc)
		assert.Error(t, err, "duplicate unique key should fail")
	})

	t.Run("update schema - add index and modify validation", func(t *testing.T) {
		// First, apply initial schema
		initialConfig := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "name", Type: "string", Required: true},
							{Name: "status", Type: "string"},
						},
						AdditionalProperties: true,
						ValidationLevel:      "moderate",
						Indexes: []schema.Index{
							{Keys: []string{"name"}, Unique: true},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, initialConfig)
		require.NoError(t, err)

		// Verify initial state
		indexes, err := getIndexes(context.Background(), coll)
		require.NoError(t, err)
		assert.Len(t, indexes, 2, "should have _id_ and IL_name")

		// Update schema: add new index and field
		updatedConfig := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "name", Type: "string", Required: true},
							{Name: "status", Type: "string"},
							{Name: "category", Type: "string", Required: true},
						},
						AdditionalProperties: true,
						ValidationLevel:      "moderate",
						Indexes: []schema.Index{
							{Keys: []string{"name"}, Unique: true},
							{Keys: []string{"category"}},
						},
					},
				},
			},
		}

		err = schema.CreateOrUpdateMongoSchema(context.Background(), db, updatedConfig)
		require.NoError(t, err)

		// Verify updated state
		indexes, err = getIndexes(context.Background(), coll)
		require.NoError(t, err)
		assert.Len(t, indexes, 3, "should have _id_, IL_name, and SL_category")
		assert.Contains(t, indexes, "SL_category", "should have new category index")

		// Insert document with new required field
		doc := bson.M{
			"name":     "test1",
			"status":   "active",
			"category": "server",
		}
		_, err = coll.InsertOne(context.Background(), doc)
		assert.NoError(t, err, "document with new required field should be inserted")

		// Missing new required field should fail
		invalidDoc := bson.M{
			"name":   "test2",
			"status": "active",
		}
		_, err = coll.InsertOne(context.Background(), invalidDoc)
		assert.Error(t, err, "document missing new required field should fail")
	})

	t.Run("remove obsolete indexes", func(t *testing.T) {
		// Create schema with multiple indexes
		config1 := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "field1", Type: "string"},
							{Name: "field2", Type: "string"},
							{Name: "field3", Type: "string"},
						},
						AdditionalProperties: true,
						Indexes: []schema.Index{
							{Keys: []string{"field1"}},
							{Keys: []string{"field2"}},
							{Keys: []string{"field3"}},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, config1)
		require.NoError(t, err)

		indexes, err := getIndexes(context.Background(), coll)
		require.NoError(t, err)
		assert.Len(t, indexes, 4, "should have 4 indexes")

		// Update schema to remove field3 index
		config2 := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "field1", Type: "string"},
							{Name: "field2", Type: "string"},
						},
						AdditionalProperties: true,
						Indexes: []schema.Index{
							{Keys: []string{"field1"}},
							{Keys: []string{"field2"}},
						},
					},
				},
			},
		}

		err = schema.CreateOrUpdateMongoSchema(context.Background(), db, config2)
		require.NoError(t, err)

		indexes, err = getIndexes(context.Background(), coll)
		require.NoError(t, err)
		assert.Len(t, indexes, 3, "should have 3 indexes (field3 removed)")
		assert.NotContains(t, indexes, "SL_field3", "field3 index should be removed")
	})

	t.Run("compound index with direction", func(t *testing.T) {
		config := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "timestamp", Type: "int"},
							{Name: "user_id", Type: "string"},
						},
						AdditionalProperties: true,
						Indexes: []schema.Index{
							{
								Keys:      []string{"user_id", "timestamp"},
								Direction: []int{1, -1}, // ascending user_id, descending timestamp
							},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, config)
		require.NoError(t, err)

		indexes, err := getIndexes(context.Background(), coll)
		require.NoError(t, err)
		assert.Contains(t, indexes, "SL_user_id_timestamp_1_-1", "should have compound index with direction")
	})

	t.Run("sparse index", func(t *testing.T) {
		config := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "optional_field", Type: "string"},
						},
						AdditionalProperties: true,
						Indexes: []schema.Index{
							{
								Keys:   []string{"optional_field"},
								Sparse: true,
							},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, config)
		require.NoError(t, err)

		indexes, err := getIndexes(context.Background(), coll)
		require.NoError(t, err)
		assert.Contains(t, indexes, "SPARSE_optional_field", "should have sparse index")

		// Verify sparse index allows multiple documents without the field
		ctx := context.Background()
		_, err = coll.InsertOne(ctx, bson.M{"other_field": "value1"})
		assert.NoError(t, err)
		_, err = coll.InsertOne(ctx, bson.M{"other_field": "value2"})
		assert.NoError(t, err, "sparse index should allow multiple docs without indexed field")
	})

	t.Run("disable validation when schema is nil", func(t *testing.T) {
		// First apply a schema with validation
		configWithValidation := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "required_field", Type: "string", Required: true},
						},
						AdditionalProperties: false,
						Indexes: []schema.Index{
							{Keys: []string{"required_field"}},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, configWithValidation)
		require.NoError(t, err)

		// Verify validation is enforced
		_, err = coll.InsertOne(context.Background(), bson.M{"other_field": "value"})
		assert.Error(t, err, "validation should be enforced")

		// Now disable validation by setting schema to nil
		configWithoutValidation := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: nil,
				},
			},
		}

		err = schema.CreateOrUpdateMongoSchema(context.Background(), db, configWithoutValidation)
		require.NoError(t, err)

		// Verify validation is disabled
		_, err = coll.InsertOne(context.Background(), bson.M{"any_field": "any_value"})
		assert.NoError(t, err, "validation should be disabled")
	})

	t.Run("case validation with global config", func(t *testing.T) {
		config := schema.Config{
			Global: schema.Global{
				SchemaValidationConfig: struct {
					Case schema.Case `yaml:"case"`
				}{
					Case: schema.Case{Strict: true, Type: "lower"},
				},
			},
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{Name: "lowercase_field", Type: "string", Required: true},
						},
						AdditionalProperties: true, // Changed to true to allow _id field
						Indexes: []schema.Index{
							{Keys: []string{"lowercase_field"}},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, config)
		require.NoError(t, err)

		ctx := context.Background()

		// Valid lowercase value
		_, err = coll.InsertOne(ctx, bson.M{"lowercase_field": "alllowercase123"})
		assert.NoError(t, err, "lowercase value should be accepted")

		// Invalid uppercase value
		_, err = coll.InsertOne(ctx, bson.M{"lowercase_field": "HasUpperCase"})
		assert.Error(t, err, "uppercase characters should fail validation")
	})

	t.Run("field with pattern validation", func(t *testing.T) {
		config := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{
								Name:    "email",
								Type:    "string",
								Pattern: "^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$",
							},
						},
						AdditionalProperties: true,
						Indexes: []schema.Index{
							{Keys: []string{"email"}},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, config)
		require.NoError(t, err)

		ctx := context.Background()

		// Valid email
		_, err = coll.InsertOne(ctx, bson.M{"email": "user@example.com"})
		assert.NoError(t, err, "valid email should be accepted")

		// Invalid email
		_, err = coll.InsertOne(ctx, bson.M{"email": "not-an-email"})
		assert.Error(t, err, "invalid email should fail validation")
	})

	t.Run("field dependencies", func(t *testing.T) {
		config := schema.Config{
			Entities: map[string]schema.EntitySchema{
				testEntityType: {
					Schema: &schema.Schema{
						Fields: []schema.Field{
							{
								Name:       "cluster_mode",
								Type:       "string",
								Dependents: []string{"shard_count", "replica_count"},
							},
							{Name: "shard_count", Type: "int"},
							{Name: "replica_count", Type: "int"},
						},
						AdditionalProperties: true,
						Indexes: []schema.Index{
							{Keys: []string{"cluster_mode"}},
						},
					},
				},
			},
		}

		err := schema.CreateOrUpdateMongoSchema(context.Background(), db, config)
		require.NoError(t, err)

		ctx := context.Background()

		// Document with cluster_mode must have dependent fields
		_, err = coll.InsertOne(ctx, bson.M{
			"cluster_mode":  "enabled",
			"shard_count":   int64(3),
			"replica_count": int64(2),
		})
		assert.NoError(t, err, "document with all dependent fields should be accepted")

		// Document with cluster_mode but missing dependent fields should fail
		_, err = coll.InsertOne(ctx, bson.M{
			"cluster_mode": "enabled",
			"shard_count":  int64(3),
			// missing replica_count
		})
		assert.Error(t, err, "document missing dependent field should fail")

		// Document without cluster_mode can omit dependent fields
		_, err = coll.InsertOne(ctx, bson.M{
			"other_field": "value",
		})
		assert.NoError(t, err, "document without parent field can omit dependent fields")
	})
}

// Helper function to get list of index names
func getIndexes(ctx context.Context, coll *mongo.Collection) ([]string, error) {
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var indexes []string
	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			return nil, err
		}
		name, ok := index["name"].(string)
		if !ok {
			continue
		}
		indexes = append(indexes, name)
	}
	return indexes, cursor.Err()
}
