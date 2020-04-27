/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package sqs

import "time"

// Configuration to consume an AWS SQS queue.
type Configuration struct {
	QueueURL            string        `toml:"queueURL" default:"" comment:"URL of target SQS queue"`
	MaxNumberOfMessages int64         `toml:"maxNumberOfMessages" default:"10" comment:"Max number of messages to retrieve from SQS queue"`
	VisibilityTimeout   time.Duration `toml:"visibilityTimeout" default:"2m30s" comment:"Visibility timeout of messages retrieved from SQS queue"`
	HeartbeatInterval   time.Duration `toml:"heartbeatInterval" default:"1m" comment:"Interval at which visibility timeouts are renewed"`
	WaitTime            time.Duration `toml:"waitTime" default:"20s" comment:"Wait time for long polling"`
	Forever             bool          `toml:"forever" default:"true" comment:"Continue polling when the queue is empty"`
}
