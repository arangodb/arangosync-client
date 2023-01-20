//
// Copyright 2017-2023 ArangoDB GmbH, Cologne, Germany
//
// The Programs (which include both the software and documentation) contain
// proprietary information of ArangoDB GmbH; they are provided under a license
// agreement containing restrictions on use and disclosure and are also
// protected by copyright, patent and other intellectual and industrial
// property laws. Reverse engineering, disassembly or decompilation of the
// Programs, except to the extent required to obtain interoperability with
// other independently created software or as specified by law, is prohibited.
//
// It shall be the licensee's responsibility to take all appropriate fail-safe,
// backup, redundancy, and other measures to ensure the safe use of
// applications if the Programs are used for purposes such as nuclear,
// aviation, mass transit, medical, or other inherently dangerous applications,
// and ArangoDB GmbH disclaims liability for any damages caused by such use of
// the Programs.
//
// This software is the confidential and proprietary information of ArangoDB
// GmbH. You shall not disclose such confidential and proprietary information
// and shall use it only in accordance with the terms of the license agreement
// you entered into with ArangoDB GmbH.
//

package retry

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
)

var (
	maskAny = errors.WithStack
)

type permanentError struct {
	Err error
}

func (e *permanentError) Error() string {
	return e.Err.Error()
}

func (e *permanentError) Cause() error {
	return e.Err
}

// Permanent makes the given error a permanent failure
// that stops the Retry loop immediately.
func Permanent(err error) error {
	return &permanentError{Err: err}
}

// GetCausePermanentError returns cause of a permanent error.
// When a given error is not permanent then original error is returned.
func GetCausePermanentError(err error) error {
	if e, ok := isPermanent(err); ok {
		return e.Cause()
	}

	return err
}

func isPermanent(err error) (*permanentError, bool) {
	type causer interface {
		Cause() error
	}

	for err != nil {
		if pe, ok := err.(*permanentError); ok {
			return pe, true
		}
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return nil, false
}

// retry the given operation until it succeeds,
// has a permanent failure or times out.
func retry(op func() error, b backoff.BackOff) error {
	var failure error
	wrappedOp := func() error {
		if err := op(); err == nil {
			return nil
		} else if pe, ok := isPermanent(err); ok {
			// Detected permanent error.
			failure = pe.Err
			return nil
		} else {
			return err
		}
	}

	if err := backoff.Retry(wrappedOp, b); err != nil {
		return err
	}
	if failure != nil {
		return errors.WithMessage(failure, "permanent error")
	}
	return nil
}

// Retry the given operation until it succeeds,
// has a permanent failure or times out.
func Retry(op func() error, timeout time.Duration) error {
	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = timeout
	eb.MaxInterval = timeout / 3
	return retry(op, eb)
}

// Func is a function to be performed with retry logic.
type Func func(ctx context.Context) error

// RetryWithContext retries the given operation until it succeeds,
// has a permanent failure or times out.
// The timeout is the minimum between the timeout of the context and the given timeout.
// The context given to the operation will have a timeout of a percentage of the overall timeout.
// The percentage is calculated from the given minimum number of attempts.
// If the given minimum number of attempts is 3, the timeout of each `op` call if the overall timeout / 3.
// The default minimum number of attempts is 2.
func RetryWithContext(ctx context.Context, op Func, timeout time.Duration, minAttempts ...int) error {
	deadline, ok := ctx.Deadline()
	if ok {
		ctxTimeout := time.Until(deadline)
		if ctxTimeout < timeout {
			timeout = ctxTimeout
		}
	}
	divider := 2
	if len(minAttempts) == 1 {
		divider = minAttempts[0]
	}
	ctxOp := func() error {
		ctxInner, cancel := context.WithTimeout(ctx, timeout/time.Duration(divider))
		defer cancel()
		if err := op(ctxInner); err != nil {
			return maskAny(err)
		}
		return nil
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = timeout
	eb.MaxInterval = timeout / 3
	b := backoff.WithContext(eb, ctx)
	if err := retry(ctxOp, b); err != nil {
		return maskAny(err)
	}
	return nil
}

// WithTimeoutContext retries the given operation with exponential backoff until it succeeds.
// It breaks if `op` functions returns permanent error or when the given context is canceled.
// The timeout is the minimum between the timeout of the context and the given timeout.
func WithTimeoutContext(ctx context.Context, op Func, timeout time.Duration) error {
	if deadline, ok := ctx.Deadline(); ok {
		ctxTimeout := time.Until(deadline)
		if ctxTimeout < timeout {
			timeout = ctxTimeout
		}
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	ctxOp := func() error {
		return op(ctx)
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = timeout
	eb.MaxInterval = timeout / 3
	b := backoff.WithContext(eb, ctx)
	return retry(ctxOp, b)
}

// WithTimeoutContextAndInterval acts the same as WithTimeoutContext but with constant backoff
func WithTimeoutContextAndInterval(ctx context.Context, op Func, timeout time.Duration, interval time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	ctxOp := func() error {
		return op(ctx)
	}

	b := backoff.WithContext(backoff.NewConstantBackOff(interval), ctx)
	return retry(ctxOp, b)
}
