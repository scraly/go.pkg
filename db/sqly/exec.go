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
	"context"
	"fmt"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"

	pkgdb "github.com/scraly/go.pkg/db"
)

// Mutate executes the given query and checks that at least one row has been affected.
//
// Returns ErrNoModification is no row has been affected.
func Mutate(ctx context.Context, db sqlx.ExecerContext, sqlizer sq.Sqlizer) error {
	query, args, err := sqlizer.ToSql()
	if err != nil {
		return fmt.Errorf("sqly: unable to build query: %w", err)
	}

	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("sqly: unable to execute query: %w", err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqly: unable to retrieve query result: %w", err)
	}

	if count == 0 {
		return pkgdb.ErrNoModification
	}

	return nil
}

// ExecCount executes the given count query.
//
// The given query is expected to be a SELECT COUNT(*).
func ExecCount(ctx context.Context, db sqlx.QueryerContext, sqlizer sq.Sqlizer) (int, error) {
	query, args, err := sqlizer.ToSql()
	if err != nil {
		return 0, fmt.Errorf("sqly: unable to build query: %w", err)
	}

	var count int
	err = db.QueryRowxContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("sqly: unable to execute query: %w", err)
	}

	return count, nil
}

// Search from the given table with the given criteria.
//
// The selectBuilder parameter is expected to only define selected columns and ORDER BYs, other
// parameters are automatically added to the SELECT query.
// Parameters from and where are specified separately because they are used to build a count query
// in case pagination is enabled.
//
// If pagination is not nil, only the requested page is returned along with the total number of
// results. Otherwise, countBuilder is not used and all results are returned.
//
// The dest parameter must be a pointer to a slice.
func Search(ctx context.Context, db sqlx.QueryerContext, countBuilder sq.SelectBuilder, selectBuilder sq.SelectBuilder, from string, where interface{}, pagination *pkgdb.Pagination, dest interface{}) (int, error) {
	// Check the destination type
	destType := reflect.TypeOf(dest)
	if destType.Kind() != reflect.Ptr || destType.Elem().Kind() != reflect.Slice {
		return 0, fmt.Errorf("sqly: invalid destination type, a slice pointer is expected")
	}

	// Initialize the SELECT statement
	sqlizer := selectBuilder.
		From(from).
		Where(where)

	if pagination == nil {
		// If pagination is disabled, execute the complete SELECT and count returned results
		query, args, err := sqlizer.ToSql()
		if err != nil {
			return 0, fmt.Errorf("sqly: unable to build query: %w", err)
		}

		err = sqlx.SelectContext(ctx, db, dest, query, args...)
		if err != nil {
			return 0, fmt.Errorf("sqly: unable to execute query: %w", err)
		}

		return reflect.ValueOf(dest).Elem().Len(), nil
	}

	// Count the total number of items matching given criteria
	count, err := ExecCount(ctx, db, countBuilder.
		From(from).
		Where(where))

	if err != nil {
		return 0, fmt.Errorf("sqly: unable to count total results: %w", err)
	}

	pagination.SetTotal(uint(count))

	// Add pagination to the SELECT statement and execute the query
	query, args, err := sqlizer.
		Offset(uint64(pagination.Offset())).
		Limit(uint64(pagination.PerPage)).
		ToSql()

	if err != nil {
		return 0, fmt.Errorf("sqly: unable to build query: %w", err)
	}

	err = sqlx.SelectContext(ctx, db, dest, query, args...)
	if err != nil {
		return 0, fmt.Errorf("sqly: unable to execute query: %w", err)
	}

	return count, nil
}
