/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package sqsmock

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type sqsMock struct {
	sqsiface.SQSAPI

	ctx      context.Context
	requests chan<- interface{}
}

// New creates a new SQS mock.
func New(ctx context.Context, requests chan<- interface{}) sqsiface.SQSAPI {
	return &sqsMock{
		ctx:      ctx,
		requests: requests,
	}
}

type receiveMessageResponse struct {
	output *sqs.ReceiveMessageOutput
	err    error
}

// ReceiveMessageRequest from the SQS mock.
type ReceiveMessageRequest struct {
	*sqs.ReceiveMessageInput
	response chan<- receiveMessageResponse
}

// Reply to a ReceiveMessageRequest from the SQS mock.
func (r ReceiveMessageRequest) Reply(output *sqs.ReceiveMessageOutput, err error) {
	r.response <- receiveMessageResponse{output, err}
	close(r.response)
}

func (m *sqsMock) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, options ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	responseChan := make(chan receiveMessageResponse, 1)

	// Send request
	select {
	case m.requests <- ReceiveMessageRequest{input, responseChan}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Receive response
	select {
	case response := <-responseChan:
		return response.output, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *sqsMock) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return m.ReceiveMessageWithContext(m.ctx, input)
}

type changeMessageVisibilityResponse struct {
	output *sqs.ChangeMessageVisibilityOutput
	err    error
}

// ChangeMessageVisibilityRequest from the SQS mock.
type ChangeMessageVisibilityRequest struct {
	*sqs.ChangeMessageVisibilityInput
	response chan<- changeMessageVisibilityResponse
}

// Reply to a ChangeMessageVisibilityRequest from the SQS mock.
func (r ChangeMessageVisibilityRequest) Reply(output *sqs.ChangeMessageVisibilityOutput, err error) {
	r.response <- changeMessageVisibilityResponse{output, err}
	close(r.response)
}

func (m *sqsMock) ChangeMessageVisibilityWithContext(ctx aws.Context, input *sqs.ChangeMessageVisibilityInput, options ...request.Option) (*sqs.ChangeMessageVisibilityOutput, error) {
	responseChan := make(chan changeMessageVisibilityResponse, 1)

	// Send request
	select {
	case m.requests <- ChangeMessageVisibilityRequest{input, responseChan}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Receive response
	select {
	case response := <-responseChan:
		return response.output, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *sqsMock) ChangeMessageVisibility(input *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	return m.ChangeMessageVisibilityWithContext(m.ctx, input)
}

type deleteMessageResponse struct {
	output *sqs.DeleteMessageOutput
	err    error
}

// DeleteMessageRequest from the SQS mock.
type DeleteMessageRequest struct {
	*sqs.DeleteMessageInput
	response chan<- deleteMessageResponse
}

// Reply to a DeleteMessageRequest from the SQS mock.
func (r DeleteMessageRequest) Reply(output *sqs.DeleteMessageOutput, err error) {
	r.response <- deleteMessageResponse{output, err}
	close(r.response)
}

func (m *sqsMock) DeleteMessageWithContext(ctx aws.Context, input *sqs.DeleteMessageInput, options ...request.Option) (*sqs.DeleteMessageOutput, error) {
	responseChan := make(chan deleteMessageResponse, 1)

	// Send request
	select {
	case m.requests <- DeleteMessageRequest{input, responseChan}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Receive response
	select {
	case response := <-responseChan:
		return response.output, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *sqsMock) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return m.DeleteMessageWithContext(m.ctx, input)
}
