// Package grpczaphelper implements multiple helper function
package grpczaphelper

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
)

// CodeToLevelDebug implements a configuration of https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/logging/zap with debug level instead of info
func CodeToLevelDebug(code codes.Code) zapcore.Level {
	switch code {
	case codes.OK, codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.Unauthenticated:
		return zap.DebugLevel
	case codes.Unknown:
		return zap.ErrorLevel
	case codes.DeadlineExceeded:
		return zap.WarnLevel
	case codes.PermissionDenied:
		return zap.WarnLevel
	case codes.ResourceExhausted:
		return zap.WarnLevel
	case codes.FailedPrecondition:
		return zap.WarnLevel
	case codes.Aborted:
		return zap.WarnLevel
	case codes.OutOfRange:
		return zap.WarnLevel
	case codes.Unimplemented:
		return zap.ErrorLevel
	case codes.Internal:
		return zap.ErrorLevel
	case codes.Unavailable:
		return zap.WarnLevel
	case codes.DataLoss:
		return zap.ErrorLevel
	default:
		return zap.ErrorLevel
	}
}
