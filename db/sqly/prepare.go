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
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// PrepareNamed prepares a named statement using the correct bindvar type.
//
// This is a replacement for sqlx.DB.PrepareNamed, which fails to autodetect the bindvar type if
// the driver name is unknown (for example, when wrapping a driver with ocsql).
//
// This implementation relies on squirrel to properly inject binvars. Each argument must have a
// string value starting with a colon, which defines the name of the parameter in the named
// statement.
//
// Example:
//
//     sqly.PrepareNamed(sqlxDB, sq.Select("*").From("licenses").Where(sq.Eq{"license_id": ":id"}))
//
// Is equivalent to:
//
//     sqlxDB.PrepareNamed("SELECT * FROM licenses WHERE license_id=:id")
//
func PrepareNamed(db sqlx.Preparer, sqlizer sq.Sqlizer) (*sqlx.NamedStmt, error) {
	// Generate SQL and extract args
	queryString, args, err := sqlizer.ToSql()
	if err != nil {
		return nil, err
	}

	// Deduce parameter names from extracted args
	params := make([]string, len(args))
	for i, arg := range args {
		name, ok := arg.(string)
		if !ok {
			return nil, fmt.Errorf("sqly: invalid arg type: expected string but got %T", arg)
		}
		if !strings.HasPrefix(name, ":") {
			return nil, fmt.Errorf("sqly: invalid param name: should start with a colon but got %v", name)
		}
		params[i] = name[1:]
	}

	// Prepare the statement
	stmt, err := sqlx.Preparex(db, queryString)
	if err != nil {
		return nil, err
	}

	// Craft the NamedStmt
	return &sqlx.NamedStmt{
		Params:      params,
		QueryString: queryString,
		Stmt:        stmt,
	}, nil
}
