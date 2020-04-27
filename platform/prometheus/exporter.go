/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package prometheus

import (
	"context"
	"net/http"

	"contrib.go.opencensus.io/exporter/prometheus"

	"github.com/scraly/go.pkg/log"
	"go.opencensus.io/stats/view"
	"golang.org/x/xerrors"
)

// newExporter creates a new, configured Prometheus exporter.
func newExporter(config Config) (*prometheus.Exporter, error) {
	exporter, err := prometheus.NewExporter(prometheus.Options{
		Namespace: config.Namespace,
		OnError: func(err error) {
			log.CheckErr("Error occurred in Prometheus exporter", err)
		},
	})

	return exporter, err
}

// RegisterExporter adds prometheus exporter
func RegisterExporter(ctx context.Context, conf Config, r *http.ServeMux) (func() error, error) {
	// Start prometheus
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	exporter, err := newExporter(conf)
	if err != nil {
		return nil, xerrors.Errorf("platform: unable to register prometheus exporter: %w", err)
	}

	// Add exporter
	view.RegisterExporter(exporter)

	// Add metrics handler
	r.Handle("/metrics", exporter)

	// No error
	return nil, nil
}
