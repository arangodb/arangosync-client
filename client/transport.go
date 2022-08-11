//
// Copyright 2021-2022 ArangoDB GmbH, Cologne, Germany
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

package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/proxy"
)

// ConnectionWrapper instructs how to wrap net.Conn connection.
type ConnectionWrapper func(net.Conn) net.Conn

type customDialer struct {
	dialer  *net.Dialer
	wrapper ConnectionWrapper
}

// DialContext gets connection from the net.Dialer and wraps it with the custom connection.
// which was provided in NewCustomDialer.
func (d *customDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	childConnection, err := d.dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	return d.wrapper(childConnection), nil
}

// NewHTTPTransport returns default HTTP1 transport.
func NewHTTPTransport(connectionWrapper ConnectionWrapper) *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           newContextDialer(connectionWrapper).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// NewArangoSyncHTTPTransport returns arangosync HTTP transport.
func NewArangoSyncHTTPTransport(tlsConfig *tls.Config, connectionWrapper ConnectionWrapper) *http.Transport {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           newContextDialer(connectionWrapper).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   90 * time.Second,
		TLSClientConfig:       tlsConfig,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return transport
}

func newContextDialer(connectionWrapper ConnectionWrapper) proxy.ContextDialer {
	netDialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if connectionWrapper != nil {
		return &customDialer{
			dialer:  netDialer,
			wrapper: connectionWrapper,
		}
	}
	return netDialer
}
