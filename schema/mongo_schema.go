package schema

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	logDebugLevel = 10

	// Validation level of "moderate" allows for existing invalid documents to be
	// bypassed during updates. This should allow for more flexible schema migration
	// rollouts compared to "strict".
	// See: https://docs.aws.amazon.com/documentdb/latest/developerguide/json-schema-validation.html
	defaultJSONSchemaValidationLevel = "moderate"
	defaultIndexDirection            = 1

	regexLowerCase = `^[a-z0-9\W_]+$`
	regexRFC3339   = `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)$`
	regexInt64     = `^(-?(0|[1-9]\d{0,18})|922337203685477580[0-7]|-9223372036854775808)$`
)

var (
	errNoKeysForIndex              = errors.New("no keys defined for index")
	errTooManyKeysForIndex         = errors.New("too many keys defined for index; max is 30")
	errKeysAndDirectionsDoNotMatch = errors.New("number of keys and directions do not match for index")
	errInvalidIndexDirection       = errors.New("invalid direction for key(s) in index; must be 1 or -1")
	errIndexSparseAndUnique        = errors.New("index cannot be both sparse and unique")
	errInvalidFieldType            = errors.New("unsupported field type; only string, int, bool, object are supported")
	errEnumNotString               = errors.New("enums are only supported for string types")
	errFieldNameEmpty              = errors.New("field name cannot be empty")
)

// CreateOrUpdateMongoSchema creates or updates the MongoDB schema for the given entity. If the schema is nil or has
// empty fields, it removes the JSON schema validation. If the schema is not nil, it ensures that the indexes in the
// schem exists, and any indexes that are not in the schema are removed. Entity Collection creation is handled by the
// index creation process. We assume that any reasonaby designed schema should not solely rely on full collection scans.
func CreateOrUpdateMongoSchema(ctx context.Context, db *mongo.Database, config Config) error {
	log.Printf("INFO: walking through entity validations")
	for entity, validations := range config.Entities {
		log.Printf("INFO: Creating or updating schema for %s", entity)

		// New entity collections are created upon the first attempt to create a unique index. If there are no
		// indexes defined, there's no automated creation of the collection.

		// If the schema is nil, we assume the entity owner wants to bypass Schema validation type.
		// Disable JSON schema validation and move on. For safety, we don't touch the indexes in
		// case it's not intended to be managed by `schema` type validation.
		if validations.Schema == nil {
			log.Printf("INFO: No `schema` type validation defined for %s. Validators associated with the entity collection will be removed", entity)
			if err := disableMongoJSONValidation(ctx, db, entity); err != nil {
				return err
			}

			continue
		}

		log.Printf("INFO: Ensuring the %d defined indexes for %s exists", len(validations.Schema.Indexes), entity)
		if err := updateMongoIndexes(ctx, db, entity, validations.Schema.Indexes); err != nil {
			return errors.Wrapf(err, "failed to ensure index creation for %s", entity)
		}

		log.Printf("INFO: Updating JSON schema validation for %s", entity)
		if err := updateMongoJSONValidation(ctx, db, entity, *validations.Schema, config.Global); err != nil {
			return err
		}
	}

	log.Printf("INFO: Schema updated successfully")
	return nil
}

func disableMongoJSONValidation(ctx context.Context, db *mongo.Database, entity string) error {
	command := bson.D{
		{Key: "collMod", Value: entity},
		{Key: "validator", Value: bson.D{}},
		{Key: "validationLevel", Value: "off"},
	}
	if err := db.RunCommand(ctx, command).Err(); err != nil {
		return errors.Wrapf(err, "failed to remove JSON schema validation for entity %s", entity)
	}
	return nil
}

func updateMongoIndexes(ctx context.Context, db *mongo.Database, entity string, indexes []Index) error {
	// If there are no indexes defined, we assume that this is a mistake and return an error.
	// There should not be a reasonable use case for an entity that depends solely on full collection scans.
	if len(indexes) == 0 {
		return fmt.Errorf("no indexes defined for %s; at least one index should be defined for any entity", entity)
	}

	coll := db.Collection(entity)

	// Index deletion and creation both should be idempotent operations and should not cause
	// any issues if multiple processes are trying to drop the same index.
	createdIndexes := make(map[string]struct{})
	for _, index := range indexes {
		idxName, err := createIndex(ctx, coll, index)
		if err != nil {
			if strings.Contains(errors.Cause(err).Error(), "Existing index build in progress on the same collection") {
				log.Printf("WARN: Index build in progress for %s. Skipping rest of index creation because of database limit", coll.Name())
				break
			}

			return err
		}
		createdIndexes[idxName] = struct{}{}
	}

	log.Printf("INFO: Checking if any non-system indexes need to be dropped for %s", coll.Name())
	// If any indexes exist for the entity collection that are not in the schema, or
	// are not system indexes, we assume the user wants to drop them or they are
	// obsolete and should be removed.
	existing, err := existingIndexes(ctx, coll)
	if err != nil {
		return errors.Wrapf(err, "failed to get existing indexes for %s", coll.Name())
	}
	// NOTE: This is kind of a critical section that is not testable in the current
	// code structure. The logic is quite simple at this point and reads much easier as is.
	// But if we ever expand beyond a simple set check and string prefix check, we should
	// consider refactoring this into a more testable structure.
	for _, idx := range existing {
		if _, ok := createdIndexes[idx]; !ok && !strings.HasPrefix(idx, "_") {
			log.Printf("WARN: Index %s is not in the schema and not a system index. Will drop", idx)
			// DocumentDB only allows one index build at a time for a collection, whether that is
			// a create or drop. This means that if we try to drop an index while another index is
			// being built, the database will return an error. For the interim, rely on testing for
			// the declared configurations to ensure only one index change per collection. The
			// handling of this could be subject to change depending on how we end up implementing
			// the onboarding to block-etre from ods-etre.
			// See: https://docs.aws.amazon.com/documentdb/latest/developerguide/functional-differences.html
			err := coll.Indexes().DropOne(ctx, idx)
			if err != nil {
				if strings.Contains(err.Error(), "index not found") {
					log.Printf("INFO: Index %s not found. It may have been dropped by another process", idx)
					continue
				}

				return errors.Wrapf(err, "failed to drop index %s for %s", idx, coll.Name())
			}
		}
	}

	return nil
}

func createIndex(ctx context.Context, coll *mongo.Collection, index Index) (string, error) {
	// Handle all index configuration errors up front.
	if len(index.Keys) == 0 {
		return "", errors.Wrapf(errNoKeysForIndex, "index: %s", index)
	}
	if len(index.Keys) > 30 {
		return "", errors.Wrapf(errTooManyKeysForIndex, "index: %s", index)
	}
	if len(index.Direction) > 0 && len(index.Keys) != len(index.Direction) {
		return "", errors.Wrapf(errKeysAndDirectionsDoNotMatch, "index: %s", index)
	}
	if index.Sparse && index.Unique {
		return "", errors.Wrapf(errIndexSparseAndUnique, "index: %s", index)

	}
	// Validate the direction values.
	for _, direction := range index.Direction {
		if direction != 1 && direction != -1 {
			return "", errors.Wrapf(errInvalidIndexDirection, "index: %s", index)
		}
	}

	log.Printf("INFO: Creating index %s ", index)
	name := indexName(index)
	bsonIndex := toBSONIndex(index)
	mod := mongo.IndexModel{
		Keys:    bsonIndex,
		Options: options.Index().SetUnique(index.Unique).SetSparse(index.Sparse).SetName(name),
	}
	if _, err := coll.Indexes().CreateOne(ctx, mod); err != nil {
		return "", errors.Wrapf(err, "failed to create index %s with name %s", index, name)
	}

	log.Printf("INFO: Index %s created successfully if it did not exist before", name)
	return name, nil
}

func indexName(index Index) string {
	if len(index.Keys) == 0 {
		return ""
	}

	// Legacy naming convention for ods-etre.
	indexNamePrefix := "SL"
	if index.Unique {
		indexNamePrefix = "IL"
	} else if index.Sparse {
		indexNamePrefix = "SPARSE"
	}

	// If no direction is specified, we don't need to add it to the index name.
	if len(index.Direction) == 0 {
		return fmt.Sprintf("%s_%s", indexNamePrefix, strings.Join(index.Keys, "_"))
	}

	// If there are directions, we need to add them to the index name to ensure uniqueness.
	direction := intSliceToString(index.Direction)
	return fmt.Sprintf("%s_%s_%s", indexNamePrefix, strings.Join(index.Keys, "_"), strings.Join(direction, "_"))
}

func intSliceToString(slice []int) []string {
	stringSlice := make([]string, len(slice))
	for i, num := range slice {
		stringSlice[i] = strconv.Itoa(num)
	}

	return stringSlice
}

func toBSONIndex(index Index) bson.D {
	// The bson.D data structure is a slice of KV pairs of the key name and direction.
	// It's important to preserve the order of keys since MongoDB uses the order of keys
	// in the index model to determine how the index data is stored and retrieved. Therefore
	// it's important to use bson.D instead of bson.M or any hashmap-like structure.
	bsonIndex := bson.D{}
	// If no direction is specified, default to ascending order.
	if len(index.Direction) == 0 {
		for _, key := range index.Keys {
			bsonIndex = append(bsonIndex, bson.E{Key: key, Value: defaultIndexDirection})
		}
	} else {
		for i, key := range index.Keys {
			bsonIndex = append(bsonIndex, bson.E{Key: key, Value: index.Direction[i]})
		}
	}

	return bsonIndex
}

func existingIndexes(ctx context.Context, coll *mongo.Collection) ([]string, error) {
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to list indexes")
	}
	defer cursor.Close(ctx)

	var ret []string
	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			return nil, errors.Wrap(err, "Failed to decode index description")
		}

		log.Printf("INFO: Current exising index for %s: %+v", coll.Name(), index)
		name, ok := index["name"].(string)
		if !ok {
			return nil, fmt.Errorf("Failed to get index name for index from entity %s: %+v", coll.Name(), index)
		}
		ret = append(ret, name)
	}
	if err := cursor.Err(); err != nil {
		return nil, errors.Wrap(err, "Mongo cursor error when inspecting indexes")
	}

	return ret, nil
}

func updateMongoJSONValidation(ctx context.Context, db *mongo.Database, entity string, schema Schema, global Global) error {
	log.Printf("INFO: Generating bson schema validator for %s", entity)

	// If there are no field defined, we assume the entity owner want to bypass JSON validation and only use
	// `schema` to manage indexes. Disable JSON schema validation and move on.
	if len(schema.Fields) == 0 {
		log.Printf("INFO: No schema fields defined for %s. Validators associated with the entity collection will be removed", entity)
		return disableMongoJSONValidation(ctx, db, entity)
	}

	validator, err := BSONSchemaValidator(schema, global.SchemaValidationConfig.Case)
	if err != nil {
		return errors.Wrapf(err, "failed to create schema validator for %s", entity)
	}
	log.Printf("INFO: %s schema validator: %v", entity, validator)

	validationLevel := schema.ValidationLevel
	switch validationLevel {
	case "":
		validationLevel = defaultJSONSchemaValidationLevel
	case "moderate", "strict":
	default:
		return fmt.Errorf("invalid validation level %s for entity %s", validationLevel, entity)
	}

	// The collMod call to update the validator is atomic and idempotent. Therefore it should not cause
	// any issues if multiple processes are trying to update the same validator.
	d := bson.D{
		{Key: "collMod", Value: entity},
		{Key: "validator", Value: validator},
		{Key: "validationLevel", Value: validationLevel},
	}

	log.Printf("INFO: Updating validator for %s with collMod command with BSON document %v", entity, d)
	err = db.RunCommand(ctx, d).Err()
	return errors.Wrapf(err, "failed to update validator for entity %s", entity)
}

// BSONSchemaValidator converts a Schema into a BSON schema validator for MongoDB or DocumentDB.
func BSONSchemaValidator(schema Schema, globalCase Case) (bson.M, error) {
	// Typically we don't want to initialize this since we don't know if there will be any
	// required fields. However, the mongo client requires that the required cannot be nil.
	requiredFields := make([]string, 0)
	dependents := make(map[string][]string)
	properties := bson.M{}

	for _, field := range schema.Fields {
		if field.Name == "" {
			return nil, errors.Wrapf(errFieldNameEmpty, "field of type %s has an empty name", field.Type)
		}

		// Convert the field type to a BSON type.
		var bsonType string
		switch field.Type {
		case "string", "bool", "object":
			bsonType = field.Type
		case "int":
			// In MongoDB, long is a 64-bit integer which is the more common standard for int
			bsonType = "long"
		case "datetime", "int-str", "bool-str":
			bsonType = "string"
		default:
			return nil, errors.Wrapf(errInvalidFieldType, "field %s is of type %q", field.Name, field.Type)
		}

		// We only handle enums for strings right now.
		if field.Type != "string" && field.Enum != nil {
			return nil, errors.Wrapf(errEnumNotString, "field %s is of type %q", field.Name, field.Type)
		}

		// Build the field schema...
		fieldSchema := bson.M{
			"bsonType": bsonType,
		}

		// Determine casing rules
		effectiveCase := field.Case
		if effectiveCase == nil {
			effectiveCase = &globalCase
		}
		// Apply pattern or casing rule
		switch {
		// Custom pattern overrides any casing rules.
		case field.Pattern != "":
			fieldSchema["pattern"] = field.Pattern
		// Enum lists overrides any casing rules.
		case field.Enum != nil && len(field.Enum) > 0:
			fieldSchema["enum"] = field.Enum
		// DocumentDB does not support the "format" keyword, therefore we use the pattern keyword as
		// a workaround.
		case field.Type == "datetime":
			fieldSchema["pattern"] = regexRFC3339
		// ES CLI does not currently support the use of a actual integer type, so we temporarily use a
		// int string that conforms to long type in MongoDB, which is a 64-bit integer.
		case field.Type == "int-str":
			fieldSchema["pattern"] = regexInt64
		// ES CLI does not currently support the use of a actual integer type, so we temporarily use a
		// string that conforms to boolean value string representations.
		case field.Type == "bool-str":
			fieldSchema["enum"] = []string{"true", "false"}
		// Apply casing rules since there is no prioritized schema validations.
		case effectiveCase.Strict && bsonType == "string":
			if effectiveCase.Type == "lower" {
				fieldSchema["pattern"] = regexLowerCase
			}
		}
		properties[field.Name] = fieldSchema

		if field.Required {
			requiredFields = append(requiredFields, field.Name)
		}

		if len(field.Dependents) > 0 {
			dependents[field.Name] = append([]string{}, field.Dependents...)
		}
	}

	jsonSchema := bson.M{
		"bsonType":             "object",
		"properties":           properties,
		"required":             requiredFields,
		"additionalProperties": schema.AdditionalProperties,
	}

	if len(dependents) > 0 {
		jsonSchema["dependencies"] = dependents
	}

	return bson.M{"$jsonSchema": jsonSchema}, nil
}
