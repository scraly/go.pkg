/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package sqly

import (
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx/reflectx"
)

var (
	// Mapper is a sqlx/reflectx mapper that uses "db" as struct field tag.
	Mapper = reflectx.NewMapper("db")
)

// EntityMapper allows to extract columns and values from entites.
//
// This is primarily useful to build INSERT statements based on struct field tags.
type EntityMapper struct {
	modelType reflect.Type
	model     *reflectx.StructMap

	Columns []string
}

// NewEntityMapper builds a new EntityMapper for the given type.
func NewEntityMapper(modelType reflect.Type) *EntityMapper {
	model := Mapper.TypeMap(modelType)

	columns := make([]string, 0, len(model.Names))
	for column := range model.Names {
		columns = append(columns, column)
	}

	return &EntityMapper{
		modelType: modelType,
		model:     model,

		Columns: columns,
	}
}

// Values extracts values corresponding to Columns from the given entity.
//
// The given entity must have the same type as specified in NewEntityMapper.
func (m *EntityMapper) Values(entity interface{}) []interface{} {
	entityValue := reflect.ValueOf(entity)

	if entityValue.Type() != m.modelType {
		panic(fmt.Errorf("sqly: this mapper expects type %s but the given entity has type %s", m.modelType, entityValue.Type()))
	}

	values := make([]interface{}, len(m.Columns))
	for i, column := range m.Columns {
		values[i] = reflectx.FieldByIndexesReadOnly(entityValue, m.model.Names[column].Index).Interface()
	}

	return values
}
