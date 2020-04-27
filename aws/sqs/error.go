/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package sqs

import (
	"fmt"
	"time"
)

// RetriableError allows to retry the handling of a SQS message after a delay.
type RetriableError struct {
	Delay time.Duration
}

// NewRetriableError creates a RetriableError with the given delay.
func NewRetriableError(delay time.Duration) error {
	return RetriableError{
		Delay: delay,
	}
}

func (e RetriableError) Error() string {
	return fmt.Sprintf("should retry in %s", e.Delay)
}
