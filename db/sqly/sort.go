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

	"github.com/scraly/go.pkg/db"
	pkgdb "github.com/scraly/go.pkg/db"
)

type nothing struct{}
type columnSet map[string]nothing

// OrderByBuilder allows to convert SortParameters to ORDER BYs.
type OrderByBuilder struct {
	defaultSort    db.SortParameters
	allowedColumns columnSet
}

func (b OrderByBuilder) toOrderBy(param pkgdb.SortParameter) (string, error) {
	if _, ok := b.allowedColumns[param.FieldName]; !ok {
		return "", fmt.Errorf("sqly: invalid sort column: %s", param.FieldName)
	}

	switch param.Direction {
	case pkgdb.Ascending:
		return fmt.Sprintf("%s asc", param.FieldName), nil
	case pkgdb.Descending:
		return fmt.Sprintf("%s desc", param.FieldName), nil
	default:
		return "", fmt.Errorf("sqly: invalid sort direction: %v", param.Direction)
	}
}

func (b OrderByBuilder) appendOrderBys(orderBys []string, sortedColumns columnSet, params pkgdb.SortParameters) ([]string, error) {
	for _, param := range params {
		if _, ok := sortedColumns[param.FieldName]; ok {
			continue
		}

		orderBy, err := b.toOrderBy(param)
		if err != nil {
			return nil, err
		}

		orderBys = append(orderBys, orderBy)
		sortedColumns[param.FieldName] = nothing{}
	}

	return orderBys, nil
}

// NewOrderByBuilder creates a new OrderByBuilder with a default sort and a list of allowed columns.
func NewOrderByBuilder(defaultSort pkgdb.SortParameters, columns ...string) OrderByBuilder {
	// Build a map with column names to speedup lookups
	allowedColumns := make(columnSet, len(columns))
	for _, column := range columns {
		allowedColumns[column] = nothing{}
	}

	return OrderByBuilder{
		defaultSort:    defaultSort,
		allowedColumns: allowedColumns,
	}
}

// Build a list of ORDER BYs from the given SortParameters.
func (b OrderByBuilder) Build(params pkgdb.SortParameters) ([]string, error) {
	maxSize := len(b.defaultSort) + len(params)
	orderBys := make([]string, 0, maxSize)
	sortedColumns := make(columnSet, maxSize)

	orderBys, err := b.appendOrderBys(orderBys, sortedColumns, params)
	if err != nil {
		return nil, err
	}

	return b.appendOrderBys(orderBys, sortedColumns, b.defaultSort)
}
