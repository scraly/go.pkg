/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package prometheus

// Config holds information for configuring the Prometheus exporter
type Config struct {
	Namespace string `toml:"namespace" comment:"Prometheus namespace"`
}

// Validate checks that the configuration is valid.
func (c Config) Validate() error {
	return nil
}
