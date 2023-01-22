//
// Copyright 2017-2022 ArangoDB GmbH, Cologne, Germany
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
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	defaultHTTPTimeout = time.Minute * 2
)

// DefaultArangoSyncHTTPClient creates a new HTTP client configured for accessing arangosync servers.
// The variable `internal` should be set `true` if the connection within one DC.
func DefaultArangoSyncHTTPClient(tlsConfig *tls.Config, internal bool) *http.Client {
	if tlsConfig == nil {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	wrapper := func(c net.Conn) net.Conn { return c }

	return &http.Client{
		// Don't set Timeout, because it will not be possible to use higher timeout for a specific request.
		Transport: NewArangoSyncHTTPTransport(tlsConfig, wrapper),
	}
}

type httpClientCache struct {
	mutex   sync.Mutex
	clients map[string]*http.Client
}

func (c *httpClientCache) getHTTPClient(tlsConfig *TLSConfig, internal bool) (*http.Client, error) {
	isInternalStr := "0"
	if internal {
		isInternalStr = "1"
	}
	key := fmt.Sprintf("%s_%s", isInternalStr, tlsConfig.getHash())

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.clients == nil {
		c.clients = make(map[string]*http.Client)
	}

	if existingClient, ok := c.clients[key]; ok {
		return existingClient, nil
	}

	cfg, err := tlsConfig.getTLSConfig()
	if err != nil {
		return nil, errors.WithMessage(err, "could not create tls.Config from TLSConfig")
	}

	newClient := DefaultArangoSyncHTTPClient(cfg, internal)
	c.clients[key] = newClient
	return newClient, nil
}

var httpCache httpClientCache
