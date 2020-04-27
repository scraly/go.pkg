/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package jaeger

import "golang.org/x/xerrors"

// Config holds information necessary for sending trace to jaeger.
type Config struct {
	// CollectorEndpoint is the Jaeger HTTP Thrift endpoint.
	// For example, http://localhost:14268/api/traces?format=jaeger.thrift.
	CollectorEndpoint string `toml:"collectorEndpoint" default:"http://localhost:14268/api/traces?format=jaeger.thrift" comment:"Jaeger collector endpoint"`

	// AgentEndpoint instructs exporter to send spans to Jaeger agent at this address.
	// For example, localhost:6831.
	AgentEndpoint string `toml:"agentEndpoint" default:"localhost:6831" comment:"Jaeger agent endpoint"`

	// Username to be used if basic auth is required.
	// Optional.
	Username string `toml:"username" comment:"Jaeger authentication username"`

	// Password to be used if basic auth is required.
	// Optional.
	Password string `toml:"password" comment:"Jaeger authenication password"`

	// ServiceName is the name of the process.
	ServiceName string `toml:"serviceName" comment:"Service name"`

	// ProcessIP is used as Process tag in jaeger to report the IP and have a better representation of span
	ProcessIP string `toml:"processIP" comment:"IP to report in the \"ip\" process tag"`
}

// Validate checks that the configuration is valid.
func (c Config) Validate() error {
	if c.CollectorEndpoint == "" && c.AgentEndpoint == "" {
		return xerrors.New("jaeger: either collector endpoint or agent endpoint must be configured")
	}
	if c.ServiceName == "" {
		return xerrors.New("jaeger: service name must not be blank")
	}

	return nil
}
