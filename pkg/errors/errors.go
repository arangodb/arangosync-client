//
// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

package errors

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"

	errs "github.com/pkg/errors"

	driver "github.com/arangodb/go-driver"
)

var (
	Cause        = errs.Cause
	New          = errs.New
	WithStack    = errs.WithStack
	Wrap         = errs.Wrap
	Wrapf        = errs.Wrapf
	WithMessage  = errs.WithMessage
	WithMessagef = errs.WithMessagef
)

type timeout interface {
	Timeout() bool
}

// IsTimeout returns true if the given error is caused by a timeout error.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if t, ok := errs.Cause(err).(timeout); ok {
		return t.Timeout()
	}
	return false
}

type temporary interface {
	Temporary() bool
}

// IsTemporary returns true if the given error is caused by a temporary error.
func IsTemporary(err error) bool {
	if err == nil {
		return false
	}
	if t, ok := errs.Cause(err).(temporary); ok {
		return t.Temporary()
	}
	return false
}

// IsEOF returns true if the given error is caused by an EOF error.
func IsEOF(err error) bool {
	err = errs.Cause(err)
	if err == io.EOF {
		return true
	}
	if ok, err := libCause(err); ok {
		return IsEOF(err)
	}
	return false
}

// IsConnectionRefused returns true if the given error is caused by an "connection refused" error.
func IsConnectionRefused(err error) bool {
	err = errs.Cause(err)
	if err, ok := err.(syscall.Errno); ok {
		return err == syscall.ECONNREFUSED
	}
	if ok, err := libCause(err); ok {
		return IsConnectionRefused(err)
	}
	return false
}

// IsConnectionReset returns true if the given error is caused by an "connection reset by peer" error.
func IsConnectionReset(err error) bool {
	err = errs.Cause(err)
	if err, ok := err.(syscall.Errno); ok {
		return err == syscall.ECONNRESET
	}
	if ok, err := libCause(err); ok {
		return IsConnectionReset(err)
	}
	return false
}

// IsContextCanceled returns true if the given error is caused by a context cancelation.
func IsContextCanceled(err error) bool {
	err = errs.Cause(err)
	if err == context.Canceled {
		return true
	}
	if ok, err := libCause(err); ok {
		return IsContextCanceled(err)
	}
	return false
}

// IsContextDeadlineExpired returns true if the given error is caused by a context deadline expiration.
func IsContextDeadlineExpired(err error) bool {
	err = errs.Cause(err)
	if err == context.DeadlineExceeded {
		return true
	}
	if ok, err := libCause(err); ok {
		return IsContextDeadlineExpired(err)
	}
	return false
}

// libCause returns the Cause of well known go library errors.
func libCause(err error) (bool, error) {
	changed := false
	for {
		switch e := err.(type) {
		case *driver.ResponseError:
			err = e.Err
			changed = true
		case *net.DNSConfigError:
			err = e.Err
			changed = true
		case *net.OpError:
			err = e.Err
			changed = true
		case *os.SyscallError:
			err = e.Err
			changed = true
		case *url.Error:
			err = e.Err
			changed = true
		default:
			return changed, err
		}
	}
}
