/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package diagnostic

// Config holds information for diagnostic handlers.
type Config struct {
	GOPS struct {
		Enabled   bool   `toml:"enabled" default:"false" comment:"Enable GOPS agent"`
		RemoteURL string `toml:"remoteDebugURL" comment:"start a gops agent on specified URL. Ex: localhost:9999"`
	}
	PProf struct {
		Enabled bool `toml:"enabled" default:"true" comment:"Enable PProf handler"`
	}
	ZPages struct {
		Enabled bool `toml:"enabled" default:"true" comment:"Enable zPages handler"`
	}
}

// Validate checks that the configuration is valid.
func (c Config) Validate() error {
	return nil
}
