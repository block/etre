# Overview
The schema validation framework for Etre uses a configuration language to define validations such as JSON schema declarations and indexes for entity types. Initially implemented in `block-etre`, it can transition to OSS Etre later.

For full context of how we arrived at this approach, see the [Schema Validation SPADE](https://docs.google.com/document/d/19Tevi7lVJe43EAj2F9LFfN-PYkCtauwoFtcva2ynYT8/edit?tab=t.wl5rum3hp9oe).
# How It Works
## Basic Structure
### Syntax
**Syntax:** YAML format with `validations` at the top level.

**Entities:** Define entity types under `entities` and use `schema` for basic JSON schema validation.
### Basic (JSON) Schema Components
#### Fields
`fields` contains the list of object fields that require some level of schema validation
- `name` (required): Field name.
- `type`: string, int, enum, object, bool.
- `required`: Field necessity for operations.
- `pattern`: Regex for field value validation.
- `enum`: Value list for fields.
- `case`: Control case enforcement. Overrides global configuration setting for `case`.
- `dependents`: List of fields that depend on this field. If this field is present, all dependent fields must also be present.

#### Indexes
**Naming Conventions**: Although naming is managed automatically by block-etre, it follows existing conventions like `{{ "SL" or "IL" }}_{{ keys }}` to distinguish between standard and unique indexes.

**Directives**:
- `keys` (required): Fields included in the index.
- `unique`: Boolean indicating uniqueness of the index.
- `direction`: Order of index keys; use 1 for ascending and -1 for descending. If not specified, defaults to ascending.

#### Additional Properties
**Additional Properties**: This allows for flexible schema definitions where fields can be added dynamically without strict validation. It is useful for cases where the schema may evolve or when dealing with semi-structured data.

#### Global Configuration
**Schema**: Specify global case in `validations` -> `config` -> `schema`.
### Example
```yaml
validations:
  entities:
    elasticache:
      schema:
        fields:
          - name: app
            type: string
            required: true
          - name: aws_account_id
            type: string
            required: true
          - name: replication_group_arn
            type: string
            required: true
            pattern: "^arn:aws:elasticache:[a-z]{2}(-[a-z0-9]{1,15}){2}:[0-9]{12}:replicationgroup:[a-zA-Z0-9-]+$"
          - name: node_arn
            required: true
            type: string
            pattern: "^arn:aws:elasticache:[a-z]{2}(-[a-z0-9]{1,15}){2}:[0-9]{12}:cluster:[a-zA-Z0-9-]+$"
          - name: engine_version
            type: string
          - name: global_datastore
            type: bool
            required: true
          - name: region
            type: string
            enum: ["us-east-1", "us-west-2"]
            required: true
          - name: replication_group_description
            type: string
            case: # This overrides the default case for this field.
              strict: false
        additional_properties: true
        indexes:
          - keys: [app]
          - keys: [replication_group_arn]
          - keys: [node_id]
            unique: true
          - keys: [app, env, region]
  config:
    schema:
      case:
        strict: true
        type: lower
```
# Custom Validations
## Extending The Schema
The `schema` validation type is built on DocumentDB’s JSON schema and indexing features. To extend it, modify the schema's in-memory struct and update the validator logic in `block-etre`
## Creating New Validations
To create custom validations, enhance the in-memory representation and adjust the YAML configuration. Implementation of the required validation logic will likely need to be done in OSS Etre, potentially as a `Plugin` or similar interface.

### Go Code By Example
Here’s how you might represent the in-memory structure:

1. **Modify the Structs**:

```go
type Config struct {
	Entities map[string]EntityValidations `yaml:"entities"`
	// Global Configurations
	Global struct {
		SchemaValidationConfig struct {
			Case Case `yaml:"case"`
		} `yaml:"schema"`
	} `yaml:"config"`
}

type EntityValidations struct {
	Schema *Schema `yaml:"schema,omitempty"`

	// This is an example of how to add additional validation types.
	ApAdditionalProperties *ApAdditionalProperties `yaml:"ap_additional_properties,omitempty"`
}
…
type ApAdditionalProperties struct {
	// Auto correct is a flag to indicate if the field should be auto-corrected.
	AutoCorrect map[string]AutoCorrect `yaml:"auto_correct"`
}

type AutoCorrect struct {
    ...
}
```

2. **YAML Configuration**:

Define how the validation appears in your YAML file:
```yaml
validations:
  entities:
    elasticache:
      schema:
        …
      ap_additional_properties:
        auto_correct:
          provision_environment:
            fill_null: helm_if_beta
  config:
    …
```
## Performance Implications
Schema validation impacts write operations in terms of resource consumption and latency, but is optimized within MongoDB. Benchmark new validations to ensure they meet performance criteria..
# Error Handling
Currently, InsertEntities and UpdateEntities in OSS Etre handle each entity separately, stopping on the first error encountered. Until batch processing is improved, handle discrete error scenarios within your application logic
# Adding and Deleting Indexes
Indexes are automatically managed based on the YAML configuration when the application starts. You can safely add numerous indexes for new entity types. However, exercise caution with existing entity types that have large data volumes, as DocumentDB limits the number of concurrent index build processes per collection to one. Both creating and deleting indexes initiate these processes. Therefore, avoid deleting a large index and creating a new one simultaneously in the same update.

In DocumentDB v5.0, all index builds are performed in the background, meaning they won't block other operations on the collection. Despite this, it's advisable to avoid index modifications on large collections during peak usage times to minimize performance impacts.
## Unique Index Considerations
Since all index creations occur in the background, duplicate key violations are detected asynchronously. This means that if you insert a document violating a unique index, the index creation request will appear to succeed initially, but the index build will eventually fail. Therefore, it's crucial to remove any duplicate documents before creating a unique index.
