package schema

import (
	"context"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestBsonSchemaValidator(t *testing.T) {
	tests := []struct {
		name       string
		schema     Schema
		caseConfig Case
		expected   bson.M
	}{
		{
			name: "global caseConfig",
			schema: Schema{
				Fields: []Field{
					{Name: "app", Type: "string", Required: true},
					{Name: "aws_account_id", Type: "string", Required: true},
				},
				AdditionalProperties: true,
			},
			caseConfig: Case{Strict: true, Type: "lower"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"app": bson.M{
							"bsonType": "string",
							"pattern":  regexLowerCase,
						},
						"aws_account_id": bson.M{
							"bsonType": "string",
							"pattern":  regexLowerCase,
						},
					},
					"required":             []string{"app", "aws_account_id"},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "custom patterns and enums",
			schema: Schema{
				Fields: []Field{
					{
						Name:    "replication_group_arn",
						Type:    "string",
						Pattern: "^arn:aws:elasticache:[a-z]{2}(-[a-z]{1,3}){3}:[0-9]{12}:replicationgroup:[a-zA-Z0-9-]+$",
					},
					{
						Name: "region",
						Type: "string",
						Enum: []string{"us-east-1", "us-west-2"},
					},
				},
				AdditionalProperties: false,
			},
			caseConfig: Case{Strict: true, Type: "lower"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"replication_group_arn": bson.M{
							"bsonType": "string",
							"pattern":  "^arn:aws:elasticache:[a-z]{2}(-[a-z]{1,3}){3}:[0-9]{12}:replicationgroup:[a-zA-Z0-9-]+$",
						},
						"region": bson.M{
							"bsonType": "string",
							"enum":     []string{"us-east-1", "us-west-2"},
						},
					},
					"required":             []string{},
					"additionalProperties": false,
				},
			},
		},
		{
			name: "override global caseConfig",
			schema: Schema{
				Fields: []Field{
					{
						Name: "userName",
						Type: "string",
						Case: &Case{Strict: true, Type: "lower"},
					},
				},
				AdditionalProperties: false,
			},
			caseConfig: Case{Strict: true, Type: "upper"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"userName": bson.M{
							"bsonType": "string",
							"pattern":  regexLowerCase,
						},
					},
					"required":             []string{},
					"additionalProperties": false,
				},
			},
		},
		{
			name: "global caseConfig wrong field type",
			schema: Schema{
				Fields: []Field{
					{
						Name: "global",
						Type: "bool",
						Case: &Case{Strict: true, Type: "lower"},
					},
				},
				AdditionalProperties: false,
			},
			caseConfig: Case{Strict: true, Type: "upper"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"global": bson.M{
							"bsonType": "bool",
						},
					},
					"required":             []string{},
					"additionalProperties": false,
				},
			},
		},
		{
			name: "global caseConfig without override",
			schema: Schema{
				Fields: []Field{
					{Name: "email", Type: "string"},
				},
				AdditionalProperties: true,
			},
			caseConfig: Case{Strict: true, Type: "lower"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"email": bson.M{
							"bsonType": "string",
							"pattern":  regexLowerCase,
						},
					},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "mixed field requirements",
			schema: Schema{
				Fields: []Field{
					{Name: "name", Type: "string", Required: true},
					{Name: "age", Type: "int"},
				},
				AdditionalProperties: false,
			},
			caseConfig: Case{Strict: false, Type: ""},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"name": bson.M{"bsonType": "string"},
						"age":  bson.M{"bsonType": "long"},
					},
					"required":             []string{"name"},
					"additionalProperties": false,
				},
			},
		},
		{
			name: "int type",
			schema: Schema{
				Fields: []Field{
					{
						Name: "age",
						Type: "int",
					},
				},
				AdditionalProperties: true,
			},
			caseConfig: Case{Strict: true, Type: "lower"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"age": bson.M{
							"bsonType": "long",
						},
					},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "datetime type",
			schema: Schema{
				Fields: []Field{
					{
						Name: "created",
						Type: "datetime",
					},
				},
				AdditionalProperties: true,
			},
			caseConfig: Case{Strict: true, Type: "lower"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"created": bson.M{
							"bsonType": "string",
							"pattern":  regexRFC3339,
						},
					},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "int-str type",
			schema: Schema{
				Fields: []Field{
					{
						Name: "shards",
						Type: "int-str",
					},
				},
				AdditionalProperties: true,
			},
			caseConfig: Case{Strict: true, Type: "lower"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"shards": bson.M{
							"bsonType": "string",
							"pattern":  regexInt64,
						},
					},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "bool-str type",
			schema: Schema{
				Fields: []Field{
					{
						Name: "is_global",
						Type: "bool-str",
					},
				},
				AdditionalProperties: true,
			},
			caseConfig: Case{Strict: true, Type: "lower"},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"is_global": bson.M{
							"bsonType": "string",
							"enum":     []string{"true", "false"},
						},
					},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "with dependents",
			schema: Schema{
				Fields: []Field{
					{
						Name:       "cluster_endpoint",
						Type:       "string",
						Dependents: []string{"shard", "slots"},
					},
				},
				AdditionalProperties: true,
			},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"cluster_endpoint": bson.M{
							"bsonType": "string",
						},
					},
					"dependencies": map[string][]string{
						"cluster_endpoint": {"shard", "slots"},
					},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "circular dependents",
			schema: Schema{
				Fields: []Field{
					{
						Name:       "cluster_endpoint",
						Type:       "string",
						Dependents: []string{"shard"},
					},
					{
						Name:       "shard",
						Type:       "int",
						Dependents: []string{"cluster_endpoint"},
					},
				},
				AdditionalProperties: true,
			},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType": "object",
					"properties": bson.M{
						"cluster_endpoint": bson.M{
							"bsonType": "string",
						},
						"shard": bson.M{
							"bsonType": "long",
						},
					},
					"dependencies": map[string][]string{
						"cluster_endpoint": {"shard"},
						"shard":            {"cluster_endpoint"},
					},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
		{
			name: "empty schema",
			schema: Schema{
				Fields:               []Field{},
				AdditionalProperties: true,
			},
			caseConfig: Case{Strict: false, Type: ""},
			expected: bson.M{
				"$jsonSchema": bson.M{
					"bsonType":             "object",
					"properties":           bson.M{},
					"required":             []string{},
					"additionalProperties": true,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := BSONSchemaValidator(test.schema, test.caseConfig)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, result)
		})
	}

	negativeTests := []struct {
		name       string
		schema     Schema
		caseConfig Case
		expected   error
	}{
		{
			name: "bad field type",
			schema: Schema{
				Fields: []Field{
					{Name: "unknown", Type: "undefinedType"},
				},
				AdditionalProperties: true,
			},
			expected: errInvalidFieldType,
		},
		{
			name: "non-string enum",
			schema: Schema{
				Fields: []Field{
					{Name: "status", Type: "int", Enum: []string{"active", "inactive", "unknown"}},
				},
				AdditionalProperties: true,
			},
			expected: errEnumNotString,
		},
		{
			name: "empty field name",
			schema: Schema{
				Fields: []Field{
					{Name: "", Type: "int"},
				},
				AdditionalProperties: true,
			},
			expected: errFieldNameEmpty,
		},
	}

	for _, test := range negativeTests {
		t.Run(test.name, func(t *testing.T) {
			_, err := BSONSchemaValidator(test.schema, test.caseConfig)
			assert.Equal(t, test.expected, errors.Cause(err))
		})
	}
}

func TestCreateIndex_NegativeTests(t *testing.T) {
	tests := []struct {
		name     string
		index    Index
		expected error
	}{
		{
			name: "no keys",
			index: Index{
				Keys: []string{},
			},
			expected: errNoKeysForIndex,
		},
		{
			name: "too many keys",
			index: Index{
				Keys: func() []string {
					fields := make([]string, 31)
					for i := 0; i < 31; i++ {
						fields[i] = "field" + strconv.Itoa(i)
					}
					return fields
				}(),
			},
			expected: errTooManyKeysForIndex,
		},
		{
			name: "invalid index direction",
			index: Index{
				Keys:      []string{"field1", "field2"},
				Direction: []int{1, 2},
			},
			expected: errInvalidIndexDirection,
		},
		{
			name: "direction length mismatch",
			index: Index{
				Keys:      []string{"field1", "field2"},
				Direction: []int{1},
			},
			expected: errKeysAndDirectionsDoNotMatch,
		},
		{
			name: "unique and sparse",
			index: Index{
				Keys:   []string{"field1", "field2"},
				Unique: true,
				Sparse: true,
			},
			expected: errIndexSparseAndUnique,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			coll := &mongo.Collection{}
			_, err := createIndex(ctx, coll, test.index)
			assert.Equal(t, test.expected, errors.Cause(err))
		})
	}
}

func TestIndexName(t *testing.T) {
	tests := []struct {
		name     string
		index    Index
		expected string
	}{
		{
			name: "unique single field index",
			index: Index{
				Keys:   []string{"node_arn"},
				Unique: true,
			},
			expected: "IL_node_arn",
		},
		{
			name: "standard single field index",
			index: Index{
				Keys: []string{"app"},
			},
			expected: "SL_app",
		},
		{
			name: "unique compound index",
			index: Index{
				Keys:   []string{"app", "region", "env"},
				Unique: true,
			},
			expected: "IL_app_region_env",
		},
		{
			name: "standard compound index",
			index: Index{
				Keys: []string{"app", "aws_account_id"},
			},
			expected: "SL_app_aws_account_id",
		},
		{
			name: "compound index with direction",
			index: Index{
				Keys:      []string{"app", "region", "env"},
				Direction: []int{1, -1, 1},
			},
			expected: "SL_app_region_env_1_-1_1",
		},
		{
			name: "sparse index",
			index: Index{
				Keys:   []string{"is_rare"},
				Sparse: true,
			},
			expected: "SPARSE_is_rare",
		},
		{
			name:     "empty index",
			index:    Index{},
			expected: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := indexName(test.index)
			assert.Equal(t, test.expected, got)
		})
	}
}

func TestToBSONIndex(t *testing.T) {
	tests := []struct {
		name     string
		input    Index
		expected bson.D
	}{
		{
			name: "single key default direction",
			input: Index{
				Keys:   []string{"field1"},
				Unique: false,
			},
			expected: bson.D{
				{Key: "field1", Value: defaultIndexDirection},
			},
		},
		{
			name: "single key with direction specified",
			input: Index{
				Keys:      []string{"field1"},
				Direction: []int{-1},
			},
			expected: bson.D{
				{Key: "field1", Value: -1},
			},
		},
		{
			name: "compound index default direction",
			input: Index{
				Keys:   []string{"field1", "field2"},
				Unique: true,
			},
			expected: bson.D{
				{Key: "field1", Value: defaultIndexDirection},
				{Key: "field2", Value: defaultIndexDirection},
			},
		},
		{
			name: "compound index with mixed directions",
			input: Index{
				Keys:      []string{"field1", "field2", "field3"},
				Direction: []int{1, -1, 1},
			},
			expected: bson.D{
				{Key: "field1", Value: 1},
				{Key: "field2", Value: -1},
				{Key: "field3", Value: 1},
			},
		},
		{
			name: "compound index same direction",
			input: Index{
				Keys:      []string{"field1", "field2"},
				Direction: []int{1, 1},
			},
			expected: bson.D{
				{Key: "field1", Value: 1},
				{Key: "field2", Value: 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := toBSONIndex(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}
