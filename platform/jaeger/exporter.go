/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package jaeger

import (
	"context"

	"github.com/scraly/go.pkg/log"
	"contrib.go.opencensus.io/exporter/jaeger"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"
)

// newExporter creates a new, configured Jaeger exporter.
func newExporter(config Config) (*jaeger.Exporter, error) {
	exporter, err := jaeger.NewExporter(jaeger.Options{
		CollectorEndpoint: config.CollectorEndpoint,
		AgentEndpoint:     config.AgentEndpoint,
		Username:          config.Username,
		Password:          config.Password,
		OnError: func(err error) {
			log.CheckErr("Error occured in Jaeger exporter", err)
		},
		Process: jaeger.Process{
			ServiceName: config.ServiceName,
			Tags:        append([]jaeger.Tag{}, jaeger.StringTag("ip", config.ProcessIP)),
		},
	})

	return exporter, err
}

// RegisterExporter add jaeger as trace exporter
func RegisterExporter(ctx context.Context, conf Config) (func(), error) {
	// Validate config first
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	// Initialize an exporter
	exporter, err := newExporter(conf)
	if err != nil {
		return nil, xerrors.Errorf("platform: failed to create jaeger exporter: %w", err)
	}

	// Register it
	trace.RegisterExporter(exporter)

	// No error
	return func() {
		exporter.Flush()
	}, nil
}
