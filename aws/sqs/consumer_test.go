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
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	pkg "github.com/scraly/go.pkg/aws/sqs"
	sqsmock "github.com/scraly/go.pkg/aws/sqs/sqsmock"

	. "github.com/onsi/gomega"
)

type consumerReceiveMessageCall struct {
	Delay  time.Duration
	Sleep  time.Duration
	Output *sqs.ReceiveMessageOutput
	Done   bool
}

type consumerMessageHandlerCall struct {
	Delay   time.Duration
	Message string
	Sleep   time.Duration
	Err     error
	Done    bool
}

type consumerChangeMessageVisibilityCall struct {
	Delay             time.Duration
	ReceiptHandle     string
	VisibilityTimeout time.Duration
	Done              bool
}

type consumerDeleteMessageCall struct {
	Delay         time.Duration
	ReceiptHandle string
	Done          bool
}

type consumerExpectedCalls struct {
	ReceiveMessage          []consumerReceiveMessageCall
	MessageHandler          []consumerMessageHandlerCall
	ChangeMessageVisibility []consumerChangeMessageVisibilityCall
	DeleteMessage           []consumerDeleteMessageCall
}

type consumerHandlerRequest struct {
	Message  string
	Response chan<- error
}

type consumerResult struct {
	Consumed int
	Err      error
}

func similarDuration(expected, actual, threshold time.Duration) bool {
	diff := actual - expected
	return -threshold < diff && diff <= threshold
}

func TestConsumer(t *testing.T) {
	// Prepare test constants
	queueURL := "foo"
	maxNumberOfMessages := int64(7)
	visibilityTimeout := 42 * time.Second
	heartbeatInterval := 100 * time.Millisecond
	waitTime := 12 * time.Second

	messages := []*sqs.Message{
		{
			MessageId:     aws.String("bar0"),
			ReceiptHandle: aws.String("baz0"),
			Body:          aws.String("qux0"),
		},
		{
			MessageId:     aws.String("bar1"),
			ReceiptHandle: aws.String("baz1"),
			Body:          aws.String("qux1"),
		},
		{
			MessageId:     aws.String("bar2"),
			ReceiptHandle: aws.String("baz2"),
			Body:          aws.String("qux2"),
		},
		{
			MessageId:     aws.String("bar3"),
			ReceiptHandle: aws.String("baz3"),
			Body:          aws.String("qux3"),
		},
		{
			MessageId:     aws.String("bar4"),
			ReceiptHandle: aws.String("baz4"),
			Body:          aws.String("qux4"),
		},
	}

	// Define test cases:
	// - Each case will run a consumer, then wait for its exit, and finally wait for an additionnal duration.
	// - All the calls to the SQS mock are checked against expected occurences.
	// - The additionnal duration mentioned above allows to catch unexpected SQS calls after the consumer has exited.
	testCases := []struct {
		Name           string
		Forever        bool
		CancelDelay    time.Duration
		ExpectedCalls  consumerExpectedCalls
		ExpectDuration time.Duration
		ExpectConsumed int
		ExpectError    bool
		EndDelay       time.Duration
	}{
		{
			Name:    "Forever",
			Forever: true,
			ExpectedCalls: consumerExpectedCalls{
				ReceiveMessage: []consumerReceiveMessageCall{
					{
						// First receive: 3 messages
						Delay: 0,
						Output: &sqs.ReceiveMessageOutput{
							Messages: messages[:3],
						},
					},
					{
						// Second receive: no message (with forever mode, this should be ignored)
						Delay: 350 * time.Millisecond,
						Output: &sqs.ReceiveMessageOutput{
							Messages: []*sqs.Message{},
						},
					},
					{
						// Third receive: 2 messages
						Delay: 350 * time.Millisecond,
						Output: &sqs.ReceiveMessageOutput{
							Messages: messages[3:],
						},
					},
				},
				MessageHandler: []consumerMessageHandlerCall{
					{
						// Handle message 0: success after beeing locked for 150 ms
						Delay:   0,
						Message: aws.StringValue(messages[0].Body),
						Sleep:   150 * time.Millisecond,
						Err:     nil,
					},
					{
						// Handle message 1: retry after beeing locked for 250 ms
						Delay:   150 * time.Millisecond,
						Message: aws.StringValue(messages[1].Body),
						Sleep:   100 * time.Millisecond,
						Err:     pkg.NewRetriableError(2520 * time.Second),
					},
					{
						// Handle message 2: success after beeing locked for 350 ms
						Delay:   250 * time.Millisecond,
						Message: aws.StringValue(messages[2].Body),
						Sleep:   100 * time.Millisecond,
						Err:     nil,
					},
					{
						// Handle message 3: error after beeing locked for 150 ms (this should release message 4 and stop the consumer with an error)
						Delay:   350 * time.Millisecond,
						Message: aws.StringValue(messages[3].Body),
						Sleep:   150 * time.Millisecond,
						Err:     fmt.Errorf("fatal error"),
					},
				},
				ChangeMessageVisibility: []consumerChangeMessageVisibilityCall{
					{
						// First heartbeat for message 0
						Delay:             100 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[0].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// First heartbeat for message 1
						Delay:             100 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[1].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// First heartbeat for message 2
						Delay:             100 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[2].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// Second heartbeat for message 1
						Delay:             200 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[1].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// Second heartbeat for message 2
						Delay:             200 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[2].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// Schedule a retry for message 1
						Delay:             250 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[1].ReceiptHandle),
						VisibilityTimeout: 2520 * time.Second,
					},
					{
						// Third heartbeat for message 2
						Delay:             300 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[2].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// First heartbeat for message 3
						Delay:             450 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[3].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// First heartbeat for message 4
						Delay:             450 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[4].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// Release message 3 after error
						Delay:             500 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[3].ReceiptHandle),
						VisibilityTimeout: 0,
					},
					{
						// Release message 4 after error on message 3
						Delay:             500 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[4].ReceiptHandle),
						VisibilityTimeout: 0,
					},
				},
				DeleteMessage: []consumerDeleteMessageCall{
					{
						// Delete message 0 after success
						Delay:         150 * time.Millisecond,
						ReceiptHandle: aws.StringValue(messages[0].ReceiptHandle),
					},
					{
						// Delete message 2 after success
						Delay:         350 * time.Millisecond,
						ReceiptHandle: aws.StringValue(messages[2].ReceiptHandle),
					},
				},
			},
			ExpectDuration: 500 * time.Millisecond,
			ExpectConsumed: 2,
			ExpectError:    true,
			EndDelay:       150 * time.Millisecond,
		},
		{
			Name:    "Oneshot",
			Forever: false,
			ExpectedCalls: consumerExpectedCalls{
				ReceiveMessage: []consumerReceiveMessageCall{
					{
						// First receive: 1 message
						Delay: 0,
						Output: &sqs.ReceiveMessageOutput{
							Messages: messages[:1],
						},
					},
					{
						// Second receive: 2 messages
						Delay: 150 * time.Millisecond,
						Output: &sqs.ReceiveMessageOutput{
							Messages: messages[1:3],
						},
					},
					{
						// Third receive: no message (without forever mode, this should stop the consumer without error)
						Delay: 400 * time.Millisecond,
						Output: &sqs.ReceiveMessageOutput{
							Messages: []*sqs.Message{},
						},
					},
				},
				MessageHandler: []consumerMessageHandlerCall{
					{
						// Handle message 0: success after beeing locked for 150 ms
						Delay:   0,
						Message: aws.StringValue(messages[0].Body),
						Sleep:   150 * time.Millisecond,
						Err:     nil,
					},
					{
						// Handle message 1: success after beeing locked for 150 ms
						Delay:   150 * time.Millisecond,
						Message: aws.StringValue(messages[1].Body),
						Sleep:   150 * time.Millisecond,
						Err:     nil,
					},
					{
						// Handle message 2: success after beeing locked for 250 ms
						Delay:   300 * time.Millisecond,
						Message: aws.StringValue(messages[2].Body),
						Sleep:   100 * time.Millisecond,
						Err:     nil,
					},
				},
				ChangeMessageVisibility: []consumerChangeMessageVisibilityCall{
					{
						// First heartbeat for message 0
						Delay:             100 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[0].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// First heartbeat for message 1
						Delay:             250 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[1].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// First heartbeat for message 2
						Delay:             250 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[2].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
					{
						// Second heartbeat for message 2
						Delay:             350 * time.Millisecond,
						ReceiptHandle:     aws.StringValue(messages[2].ReceiptHandle),
						VisibilityTimeout: visibilityTimeout,
					},
				},
				DeleteMessage: []consumerDeleteMessageCall{
					{
						// Delete message 0 after success
						Delay:         150 * time.Millisecond,
						ReceiptHandle: aws.StringValue(messages[0].ReceiptHandle),
					},
					{
						// Delete message 1 after success
						Delay:         300 * time.Millisecond,
						ReceiptHandle: aws.StringValue(messages[1].ReceiptHandle),
					},
					{
						// Delete message 2 after success
						Delay:         400 * time.Millisecond,
						ReceiptHandle: aws.StringValue(messages[2].ReceiptHandle),
					},
				},
			},
			ExpectDuration: 400 * time.Millisecond,
			ExpectConsumed: 3,
			ExpectError:    false,
			EndDelay:       150 * time.Millisecond,
		},
		{
			Name:        "Cancelled",
			Forever:     true,
			CancelDelay: 50 * time.Millisecond,
			ExpectedCalls: consumerExpectedCalls{
				ReceiveMessage: []consumerReceiveMessageCall{
					{
						Delay: 0,
						Sleep: 300 * time.Millisecond,
						Output: &sqs.ReceiveMessageOutput{
							Messages: []*sqs.Message{},
						},
					},
				},
			},
			ExpectDuration: 50 * time.Millisecond,
			ExpectConsumed: 0,
			ExpectError:    true,
			EndDelay:       150 * time.Millisecond,
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

			// The consumer context allows to test a normal cancellation of the lock
			consumerCtx, cancelConsumer := context.WithCancel(ctx)
			defer cancelConsumer() // this line is no really needed but makes the linter happy

			// Run the consumer in another goroutine, so that this one is kept for assertions:
			// - This is needed because the consumer may call the mock either synchronously or asynchronously, and we need to run assertions in both cases.
			// - The consumer output is obtained using a non-blocking channel, so that the goroutine cannot block and leak.
			handlerChan := make(chan consumerHandlerRequest)
			resultChan := make(chan consumerResult, 1)

			go func() {
				conf := &pkg.Configuration{
					QueueURL:            queueURL,
					MaxNumberOfMessages: maxNumberOfMessages,
					VisibilityTimeout:   visibilityTimeout,
					HeartbeatInterval:   heartbeatInterval,
					WaitTime:            waitTime,
					Forever:             testCase.Forever,
				}
				consumer := pkg.NewQueueConsumerWithClient(conf, mock)

				consumed, err := consumer.ConsumeMessages(consumerCtx, func(ctx context.Context, message string) error {
					response := make(chan error, 1)

					select {
					case handlerChan <- consumerHandlerRequest{message, response}:
					case <-ctx.Done():
						return ctx.Err()
					}

					select {
					case err := <-response:
						return err
					case <-ctx.Done():
						return ctx.Err()
					}
				})

				resultChan <- consumerResult{consumed, err}
			}()

			// Prepare the assertion loop
			start := time.Now()
			var end <-chan time.Time

			var cancellation <-chan time.Time
			if testCase.CancelDelay > 0 {
				cancellation = time.After(testCase.CancelDelay)
			}

			// Run the assertion loop: react on time events, mock requests, handler calls and consumer result
			for {
				select {
				case <-cancellation:
					cancelConsumer()

				case req := <-mockRequests:
					switch req := req.(type) {
					case sqsmock.ReceiveMessageRequest:
						delay := time.Now().Sub(start)

						// Match expected call
						var expected *consumerReceiveMessageCall
						for index := range testCase.ExpectedCalls.ReceiveMessage {
							call := &testCase.ExpectedCalls.ReceiveMessage[index]
							if !call.Done && similarDuration(call.Delay, delay, 20*time.Millisecond) {
								call.Done = true
								expected = call
								break
							}
						}
						g.Expect(expected).ToNot(BeNil(), "unexpected call to ReceiveMessage after %s", delay)

						// Check request
						g.Expect(aws.StringValue(req.QueueUrl)).To(Equal(queueURL))
						g.Expect(aws.Int64Value(req.MaxNumberOfMessages)).To(Equal(maxNumberOfMessages))
						g.Expect(time.Duration(aws.Int64Value(req.VisibilityTimeout)) * time.Second).To(Equal(visibilityTimeout))
						g.Expect(time.Duration(aws.Int64Value(req.WaitTimeSeconds)) * time.Second).To(Equal(waitTime))

						// Send reponse
						go func() {
							select {
							case <-time.After(expected.Sleep):
								req.Reply(expected.Output, nil)
							case <-ctx.Done():
								req.Reply(nil, ctx.Err())
							}
						}()

					case sqsmock.ChangeMessageVisibilityRequest:
						delay := time.Now().Sub(start)

						// Match expected call
						var expected *consumerChangeMessageVisibilityCall
						for index := range testCase.ExpectedCalls.ChangeMessageVisibility {
							call := &testCase.ExpectedCalls.ChangeMessageVisibility[index]
							if !call.Done && call.ReceiptHandle == aws.StringValue(req.ReceiptHandle) && similarDuration(call.Delay, delay, 20*time.Millisecond) {
								call.Done = true
								expected = call
								break
							}
						}
						g.Expect(expected).ToNot(BeNil(), "unexpected call to ChangeMessageVisibility after %s", delay)

						// Check request
						g.Expect(aws.StringValue(req.QueueUrl)).To(Equal(queueURL))
						g.Expect(time.Duration(aws.Int64Value(req.VisibilityTimeout)) * time.Second).To(Equal(expected.VisibilityTimeout))

						// Send reponse
						req.Reply(&sqs.ChangeMessageVisibilityOutput{}, nil)

					case sqsmock.DeleteMessageRequest:
						delay := time.Now().Sub(start)

						// Match expected call
						var expected *consumerDeleteMessageCall
						for index := range testCase.ExpectedCalls.DeleteMessage {
							call := &testCase.ExpectedCalls.DeleteMessage[index]
							if !call.Done && call.ReceiptHandle == aws.StringValue(req.ReceiptHandle) && similarDuration(call.Delay, delay, 20*time.Millisecond) {
								call.Done = true
								expected = call
								break
							}
						}
						g.Expect(expected).ToNot(BeNil(), "unexpected call to DeleteMessage after %s", delay)

						// Check request
						g.Expect(aws.StringValue(req.QueueUrl)).To(Equal(queueURL))

						// Send reponse
						req.Reply(&sqs.DeleteMessageOutput{}, nil)

					default:
						t.Fatalf("unexpected mock call of type %T after %s", req, time.Now().Sub(start))
					}

				case req := <-handlerChan:
					delay := time.Now().Sub(start)

					// Match expected call
					var expected *consumerMessageHandlerCall
					for index := range testCase.ExpectedCalls.MessageHandler {
						call := &testCase.ExpectedCalls.MessageHandler[index]
						if !call.Done && similarDuration(call.Delay, delay, 20*time.Millisecond) {
							call.Done = true
							expected = call
							break
						}
					}
					g.Expect(expected).ToNot(BeNil(), "unexpected call to MessageHandler after %s", delay)

					// Check request
					g.Expect(req.Message).To(Equal(expected.Message))

					// Send reponse
					go func() {
						time.Sleep(expected.Sleep)
						req.Response <- expected.Err
					}()

				case result := <-resultChan:
					g.Expect(time.Now().Sub(start)).To(BeNumerically("~", testCase.ExpectDuration, 20*time.Millisecond))
					g.Expect(result.Consumed).To(Equal(testCase.ExpectConsumed))
					if testCase.ExpectError {
						g.Expect(result.Err).To(HaveOccurred())
					} else {
						g.Expect(result.Err).ToNot(HaveOccurred())
					}
					end = time.After(testCase.EndDelay)

				case <-end:
					for _, call := range testCase.ExpectedCalls.ReceiveMessage {
						g.Expect(call.Done).To(BeTrue(), "missing call to ReceiveMessage after %s", call.Delay)
					}
					for _, call := range testCase.ExpectedCalls.MessageHandler {
						g.Expect(call.Done).To(BeTrue(), "missing call to MessageHandler after %s", call.Delay)
					}
					for _, call := range testCase.ExpectedCalls.ChangeMessageVisibility {
						g.Expect(call.Done).To(BeTrue(), "missing call to ChangeMessageVisibility after %s", call.Delay)
					}
					for _, call := range testCase.ExpectedCalls.DeleteMessage {
						g.Expect(call.Done).To(BeTrue(), "missing call to DeleteMessage after %s", call.Delay)
					}
					return
				}
			}
		})
	}
}
