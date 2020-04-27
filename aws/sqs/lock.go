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
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"go.uber.org/zap"

	"github.com/scraly/go.pkg/log"
)

type releaseRequest struct {
	retryDelay *time.Duration
}

// MessageLock holds a message which is kept invisible to other consumers until Release is called.
//
// The invibility is garanteed be a background routine which peridically resets the visibility
// timeout of the locked message.
type MessageLock struct {
	ctx               context.Context
	svc               sqsiface.SQSAPI
	queue             string
	visibilityTimeout time.Duration
	heartbeatInterval time.Duration
	message           *sqs.Message
	release           chan releaseRequest
	err               chan error
}

// NewMessageLock creates a lock for the given SQS message.
func NewMessageLock(ctx context.Context, svc sqsiface.SQSAPI, queue string, visibilityTimeout, heartbeatInterval time.Duration, message *sqs.Message) *MessageLock {
	lock := &MessageLock{
		ctx:               ctx,
		svc:               svc,
		queue:             queue,
		visibilityTimeout: visibilityTimeout,
		heartbeatInterval: heartbeatInterval,
		message:           message,
		release:           make(chan releaseRequest, 1),
		err:               make(chan error, 1),
	}
	go lock.loop()
	return lock
}

// Message returns the locked SQS message.
func (l *MessageLock) Message() *sqs.Message {
	return l.message
}

// Release the lock.
//
// If retryDelay is nil, then the locked message is deleted.
// Otherwise, the message visibility timeout is set to the given duration.
//
// Release must be called only once.
func (l *MessageLock) Release(retryDelay *time.Duration) error {
	l.release <- releaseRequest{
		retryDelay: retryDelay,
	}
	close(l.release)
	return <-l.err
}

func (l *MessageLock) loop() {
	for {
		select {
		case release := <-l.release:
			if release.retryDelay == nil {
				l.err <- l.deleteMessage()
			} else {
				l.err <- l.changeMessageVisibility(*release.retryDelay)
			}
			return

		case <-l.ctx.Done():
			_ = l.changeMessageVisibility(0)
			l.err <- l.ctx.Err()
			return

		case <-time.After(l.heartbeatInterval):
			_ = l.changeMessageVisibility(l.visibilityTimeout)
		}
	}
}

func (l *MessageLock) deleteMessage() error {
	_, err := l.svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(l.queue),
		ReceiptHandle: l.message.ReceiptHandle,
	})
	if err != nil {
		log.For(l.ctx).Error("Failed to delete message",
			zap.String("messageID", aws.StringValue(l.message.MessageId)),
			zap.Error(err),
		)
		return err
	}
	log.For(l.ctx).Info("Message deleted",
		zap.String("messageID", aws.StringValue(l.message.MessageId)),
	)
	return nil
}

func (l *MessageLock) changeMessageVisibility(visibilityTimeout time.Duration) error {
	_, err := l.svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(l.queue),
		ReceiptHandle:     l.message.ReceiptHandle,
		VisibilityTimeout: aws.Int64(int64(visibilityTimeout / time.Second)),
	})
	if err != nil {
		log.For(l.ctx).Warn("Failed to update visibility timeout",
			zap.String("messageID", aws.StringValue(l.message.MessageId)),
			zap.Duration("visibilityTimeout", visibilityTimeout),
			zap.Error(err),
		)
		return err
	}
	log.For(l.ctx).Debug("Visibility timeout updated",
		zap.String("messageID", aws.StringValue(l.message.MessageId)),
		zap.Duration("visibilityTimeout", visibilityTimeout),
	)
	return nil
}
