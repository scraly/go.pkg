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
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/scraly/go.pkg/log"
)

// MessageHandler is responsible to consume a single message.
type MessageHandler func(ctx context.Context, message string) error

// QueueConsumer allows to consume an AWS SQS queue.
type QueueConsumer struct {
	svc                 sqsiface.SQSAPI
	queue               string
	maxNumberOfMessages int64
	visibilityTimeout   time.Duration
	heartbeatInterval   time.Duration
	waitTime            time.Duration
	forever             bool
}

// NewQueueConsumer creates a QueueConsumer from the given configuration.
func NewQueueConsumer(conf *Configuration, awsSession client.ConfigProvider) *QueueConsumer {
	return NewQueueConsumerWithClient(conf, sqs.New(awsSession))
}

// NewQueueConsumerWithClient creates a QueueConsumer from the given configuration and using a
// preconfigured SQS client.
func NewQueueConsumerWithClient(conf *Configuration, sqsClient sqsiface.SQSAPI) *QueueConsumer {
	return &QueueConsumer{
		svc:                 sqsClient,
		queue:               conf.QueueURL,
		maxNumberOfMessages: conf.MaxNumberOfMessages,
		visibilityTimeout:   conf.VisibilityTimeout,
		heartbeatInterval:   conf.HeartbeatInterval,
		waitTime:            conf.WaitTime,
		forever:             conf.Forever,
	}
}

var noDelay = time.Duration(0)

func extractDelay(err error) *time.Duration {
	if err == nil {
		return nil
	}

	var retriableError RetriableError
	if xerrors.As(err, &retriableError) {
		return &retriableError.Delay
	}
	return &noDelay
}

// ConsumeMessages using the given handler.
//
// Each message is kept invisible to other consumers until its handler returns.
// Returns the count of consumed messages along with the encountered error, if any.
func (m *QueueConsumer) ConsumeMessages(ctx context.Context, handler MessageHandler) (int, error) {
	consumed := 0

	for {
		log.For(ctx).Debug("Receive messages",
			zap.String("queue", m.queue),
			zap.Int64("maxNumberOfMessages", m.maxNumberOfMessages),
			zap.Duration("visibilityTimeout", m.visibilityTimeout),
			zap.Duration("waitTime", m.waitTime),
		)

		result, err := m.svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            aws.String(m.queue),
			MaxNumberOfMessages: aws.Int64(m.maxNumberOfMessages), // The SQS-enforced maximum is 10
			VisibilityTimeout:   aws.Int64(int64(m.visibilityTimeout / time.Second)),
			WaitTimeSeconds:     aws.Int64(int64(m.waitTime / time.Second)), // The SQS-enforced maximum is 20s
		})
		if err != nil {
			log.For(ctx).Error("Failed to receive messages", zap.Error(err))
			return consumed, err
		}

		log.For(ctx).Debug("Received messages",
			zap.Int("messages", len(result.Messages)),
		)

		if len(result.Messages) == 0 {
			if m.forever {
				continue
			} else {
				break
			}
		}

		// Lock all messages
		locks := make([]*MessageLock, len(result.Messages))
		for index, message := range result.Messages {
			locks[index] = NewMessageLock(ctx, m.svc, m.queue, m.visibilityTimeout, m.heartbeatInterval, message)
		}

		// Process each message and stop at the first failure
		var firstErr error
		for _, lock := range locks {
			if firstErr != nil {
				_ = lock.Release(&noDelay)
				continue
			}

			err := handler(ctx, aws.StringValue(lock.Message().Body))
			retryDelay := extractDelay(err)

			switch {
			case retryDelay == nil:
				// No error: having retryDelay to nil marks the message as consumed, it will be deleted.
			case *retryDelay != 0:
				log.For(ctx).Warn("Schedule message processing to be retried later",
					zap.String("messageID", aws.StringValue(lock.Message().MessageId)),
					zap.Duration("retryDelay", *retryDelay),
				)
			default:
				log.For(ctx).Error("Failed to process message",
					zap.String("messageID", aws.StringValue(lock.Message().MessageId)),
					zap.Error(err),
				)
				firstErr = err
			}

			err = lock.Release(retryDelay)
			if firstErr == nil {
				if err != nil {
					firstErr = err
				} else if retryDelay == nil {
					consumed++
				}
			}
		}

		if firstErr != nil {
			return consumed, firstErr
		}
	}

	return consumed, nil
}
