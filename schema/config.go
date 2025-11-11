package schema

import "fmt"

// Configuration structures are really higher level constructs that are meant to be decoupled from
// underlying MongoDB structures. This can be moved to a package separate from the MongoDB DDL processing
// if needed in the future e.g. if we want to support other databases especially in OSS etre.

// Config represents the schema configurations for entities.
type Config struct {
	// A map of existing entities to their validation configurations.
	Entities map[string]EntitySchema `yaml:"entities"`
	// Global configuration for validation implementations that are used.
	// This is for any validation that is not specific to an entity.
	Global Global `yaml:"config"`
}

// EntitySchema represents the schema for a specific entity.
type EntitySchema struct {
	Schema *Schema `yaml:"schema,omitempty"`
}

// Schema represents the basic schema for an Entity.
// This includes JSON schema validation for entity fields as well as database index definitions.
type Schema struct {
	Fields               []Field `yaml:"fields"`
	AdditionalProperties bool    `yaml:"additional_properties"`
	Indexes              []Index `yaml:"indexes"`
	ValidationLevel      string  `yaml:"validation_level"`
}

// Field represents a single field in the schema.
// Only the name is required.
type Field struct {
	// Name is the name of the field in the schema.
	Name string `yaml:"name"`
	// Type is the type of the field. This can be string, int, bool, object, or enum.
	Type string `yaml:"type"`
	// Required indicates if the field is required in the schema.
	Required bool `yaml:"required"`
	// Pattern is a regex pattern that the field must match. This overrides any case rules.
	Pattern string `yaml:"pattern"`
	// Case is the case rules for the field. This can be strict or loose. Only case type
	// "lower" is supported right now. This overrides global case rules.
	Case *Case `yaml:"case,omitempty"`
	// Enum is a list of valid values for the field. Only string is supported right now.
	Enum []string `yaml:"enum,omitempty"`
	// Dependents is a list of field names that must also be present if this field is present.
	Dependents []string `yaml:"dependents,omitempty"`
	// Description is a human-readable description of the field.
	Description string `yaml:"description,omitempty"`
}

// Case represents the case rules for a field.
type Case struct {
	// Strict indicates if the case rules are "strict" or "loose".
	Strict bool `yaml:"strict"`
	// Type is the type of case. Only "lower" is supported right now.
	Type string `yaml:"type"`
}

// Index represents an index definition for a field or fields in the schema.
type Index struct {
	// Keys is a list of field names to be indexed.
	Keys []string `yaml:"keys"`
	// Unique indicates if the index is unique.
	Unique bool `yaml:"unique"`
	// Direction contains information the sort order of the stored index for each given key.
	// 1 for ascending, -1 for descending. If not specified, defaults to ascending. If set,
	// the number of keys and directions must match.
	Direction []int `yaml:"direction,omitempty"`
	// Sparse indicates if the index is a sparse index.
	Sparse bool `yaml:"sparse,omitempty"`
}

func (i Index) String() string {
	return fmt.Sprintf("Index{Keys: %v, Unique: %v, Direction: %v}", i.Keys, i.Unique, i.Direction)
}

// Global represents the global configuration for validation implementations that are used. Each
// validation implementation implements its own set of global configurations.
type Global struct {
	SchemaValidationConfig struct {
		Case Case `yaml:"case"`
	} `yaml:"schema"`
}
