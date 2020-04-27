/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package sqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	pkg "github.com/scraly/go.pkg/aws/sqs"
	sqsmock "github.com/scraly/go.pkg/aws/sqs/sqsmock"

	. "github.com/onsi/gomega"
)

type lockChangeMessageVisibiltyCall struct {
	Delay             time.Duration
	VisibilityTimeout time.Duration
}

type lockDeleteMessageCall struct {
	Delay time.Duration
}

func TestLock(t *testing.T) {
	// Prepare test constants
	queueURL := "foo"
	visibilityTimeout := 42 * time.Second
	heartbeatInterval := 100 * time.Millisecond
	messageID := "bar"
	receiptHandle := "baz"
	body := "qux"

	message := &sqs.Message{
		MessageId:     aws.String(messageID),
		ReceiptHandle: aws.String(receiptHandle),
		Body:          aws.String(body),
	}

	retryDelay := 2520 * time.Second

	// Define test cases:
	// - Each case will lock a message, then wait for a given duration, then release the lock and finally wait for an additionnal duration.
	// - All the calls to the SQS mock are checked against expected occurences.
	// - The additionnal duration mentioned above allows to catch unexpected SQS calls after the lock is released.
	testCases := []struct {
		Name                      string
		LockDuration              time.Duration
		CancelDelay               time.Duration
		RetryDelay                *time.Duration
		ExpectedVisibilityChanges []lockChangeMessageVisibiltyCall
		ExpectedDeletions         []lockDeleteMessageCall
		ExpectReleaseError        bool
		EndDelay                  time.Duration
	}{
		{
			Name:         "Normal",
			LockDuration: 350 * time.Millisecond,
			CancelDelay:  0,
			RetryDelay:   nil,
			ExpectedVisibilityChanges: []lockChangeMessageVisibiltyCall{
				{
					Delay:             100 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
				{
					Delay:             200 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
				{
					Delay:             300 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
			},
			ExpectedDeletions: []lockDeleteMessageCall{
				{
					Delay: 350 * time.Millisecond,
				},
			},
			ExpectReleaseError: false,
			EndDelay:           150 * time.Millisecond,
		},
		{
			Name:         "Cancelled",
			LockDuration: 350 * time.Millisecond,
			CancelDelay:  250 * time.Millisecond,
			RetryDelay:   nil,
			ExpectedVisibilityChanges: []lockChangeMessageVisibiltyCall{
				{
					Delay:             100 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
				{
					Delay:             200 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
				{
					Delay:             250 * time.Millisecond,
					VisibilityTimeout: 0,
				},
			},
			ExpectedDeletions:  []lockDeleteMessageCall{},
			ExpectReleaseError: true,
			EndDelay:           150 * time.Millisecond,
		},
		{
			Name:         "Retried",
			LockDuration: 350 * time.Millisecond,
			CancelDelay:  0,
			RetryDelay:   &retryDelay,
			ExpectedVisibilityChanges: []lockChangeMessageVisibiltyCall{
				{
					Delay:             100 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
				{
					Delay:             200 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
				{
					Delay:             300 * time.Millisecond,
					VisibilityTimeout: visibilityTimeout,
				},
				{
					Delay:             350 * time.Millisecond,
					VisibilityTimeout: retryDelay,
				},
			},
			ExpectedDeletions:  []lockDeleteMessageCall{},
			ExpectReleaseError: false,
			EndDelay:           150 * time.Millisecond,
		},
	}

	for _, item := range testCases {
		testCase := item

		t.Run(testCase.Name, func(t *testing.T) {
			g := NewWithT(t)

			// The main context ensures that all goroutines are stopped when the test exits (success or failure)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Initialize the SQS mock
			mockRequests := make(chan interface{})
			mock := sqsmock.New(ctx, mockRequests)

			// The lock context allows to test a normal cancellation of the lock
			lockCtx, cancelLock := context.WithCancel(ctx)
			defer cancelLock() // this line is no really needed but makes the linter happy

			// Run the business logic in another goroutine, so that this one is kept for assertions:
			// - This is needed because the lock may call the mock either synchronously or asynchronously, and we need to run assertions in both cases.
			// - Results of the business logic are obtained using non-blocking channels, so that the goroutine cannot block and leak.
			lockChan := make(chan *pkg.MessageLock, 1)
			errChan := make(chan error, 1)

			go func() {
				// Create the lock
				lock := pkg.NewMessageLock(lockCtx, mock, queueURL, visibilityTimeout, heartbeatInterval, message)
				lockChan <- lock

				// Wait for a given duration
				select {
				case <-time.After(testCase.LockDuration):
				case <-ctx.Done():
				}

				// Release the lock
				errChan <- lock.Release(testCase.RetryDelay)
			}()

			// Prepare the assertion loop
			start := time.Now()
			var end <-chan time.Time

			var cancellation <-chan time.Time
			if testCase.CancelDelay > 0 {
				cancellation = time.After(testCase.CancelDelay)
			}

			expectedVisibilityChanges := testCase.ExpectedVisibilityChanges
			expectedDeletions := testCase.ExpectedDeletions

			// Run the assertion loop: react on time events, mock requests and business logic outputs
			for {
				select {
				case <-cancellation:
					cancelLock()

				case req := <-mockRequests:
					switch req := req.(type) {
					case sqsmock.ChangeMessageVisibilityRequest:
						g.Expect(expectedVisibilityChanges).ToNot(BeEmpty(), "unexpected call to ChangeMessageVisibility after %s", time.Now().Sub(start))
						expectedDelay := expectedVisibilityChanges[0].Delay
						expectedVisibilityTimeout := expectedVisibilityChanges[0].VisibilityTimeout
						expectedVisibilityChanges = expectedVisibilityChanges[1:]

						g.Expect(time.Now()).To(BeTemporally("~", start.Add(expectedDelay), 20*time.Millisecond))
						g.Expect(aws.StringValue(req.QueueUrl)).To(Equal(queueURL))
						g.Expect(aws.StringValue(req.ReceiptHandle)).To(Equal(receiptHandle))
						g.Expect(time.Duration(aws.Int64Value(req.VisibilityTimeout)) * time.Second).To(Equal(expectedVisibilityTimeout))

						req.Reply(&sqs.ChangeMessageVisibilityOutput{}, nil)

					case sqsmock.DeleteMessageRequest:
						g.Expect(expectedDeletions).ToNot(BeEmpty(), "unexpected call to DeleteMessage after %s", time.Now().Sub(start))
						expectedDelay := expectedDeletions[0].Delay
						expectedDeletions = expectedDeletions[1:]

						g.Expect(time.Now()).To(BeTemporally("~", start.Add(expectedDelay), 20*time.Millisecond))
						g.Expect(aws.StringValue(req.QueueUrl)).To(Equal(queueURL))
						g.Expect(aws.StringValue(req.ReceiptHandle)).To(Equal(receiptHandle))

						req.Reply(&sqs.DeleteMessageOutput{}, nil)

					default:
						t.Fatalf("unexpected mock call of type %T after %s", req, time.Now().Sub(start))
					}

				case lock := <-lockChan:
					g.Expect(lock.Message()).To(BeIdenticalTo(message))

				case err := <-errChan:
					if testCase.ExpectReleaseError {
						g.Expect(err).To(HaveOccurred())
					} else {
						g.Expect(err).ToNot(HaveOccurred())
					}
					end = time.After(testCase.EndDelay)

				case <-end:
					g.Expect(expectedVisibilityChanges).To(BeEmpty(), "missing calls to ChangeMessageVisibility")
					g.Expect(expectedDeletions).To(BeEmpty(), "missing calls to DeleteMessageReqs")
					return
				}
			}
		})
	}
}
