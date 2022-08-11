//
// Copyright 2017-2021 ArangoDB GmbH, Cologne, Germany
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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/arangodb/arangosync-client/pkg/jwt"
	"github.com/arangodb/arangosync-client/pkg/trigger"
)

type AuthenticationConfig struct {
	JWTSecret   string
	BearerToken string
	UserName    string
	Password    string
}

var (
	sharedHTTPClient = DefaultArangoSyncHTTPClient(nil, true, 0)
)

const (
	// AllowForwardRequestHeaderKey is a request header key.
	// If this header is set, the syncmaster will forward
	// requests to the current leader instead of returning a
	// 503.
	AllowForwardRequestHeaderKey = "X-Allow-Forward-To-Leader"
)

// NewArangoSyncClient creates a new client implementation.
// The clientID can be provided to create each request with the returned client.
// The `internal` describes if the client connects to the local DC.
func NewArangoSyncClient(endpoints []string, internal bool, authConf AuthenticationConfig, tlsConfig *tls.Config,
	clientID ...string) (API, error) {
	c := &client{
		auth: authConf,
	}

	if tlsConfig != nil {
		// It is not shared client.
		c.client = DefaultArangoSyncHTTPClient(tlsConfig, internal)
	} else {
		// The shared client should be always internal connection.
		c.client = sharedHTTPClient
		c.sharedClient = true
	}

	if len(clientID) > 0 && len(clientID[0]) > 0 {
		c.clientID = clientID[0]
	}
	c.endpoints.config = endpoints
	list, err := c.endpoints.config.URLs()
	if err != nil {
		return nil, errors.Wrapf(err, "can not create URL's")
	}
	c.endpoints.urls = list
	return c, nil
}

type client struct {
	endpoints struct {
		mutex                   sync.RWMutex
		config                  Endpoint
		urls                    []url.URL
		preferredHost           string    // name:port of the current preferred endpoint
		preferredHostExpiration time.Time // After this timestamp we forget the current preferred host to re-shuflle it
		preferredHostMutex      sync.RWMutex
	}
	auth         AuthenticationConfig
	client       *http.Client
	sharedClient bool
	clientID     string
}

const (
	preferredHostTimeout = time.Hour
)

// Returns the master API (only valid when Role returns master)
func (c *client) Master() MasterAPI {
	return c
}

// Returns the worker API (only valid when Role returns worker)
func (c *client) Worker() WorkerAPI {
	return c
}

// SetShared marks the client as shared.
// Closing a shared client will not close all idle connections.
func (c *client) SetShared() {
	c.sharedClient = true
}

// Close this client
func (c *client) Close() error {
	if !c.sharedClient {
		if transport, ok := c.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	return nil
}

// Version requests the version of an arangosync instance.
func (c *client) Version(ctx context.Context) (VersionInfo, error) {
	url := c.createURLs("/_api/version", nil)

	var result VersionInfo
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return VersionInfo{}, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return VersionInfo{}, maskAny(err)
	}

	return result, nil
}

// Role requests the role of an arangosync instance.
func (c *client) Role(ctx context.Context) (Role, error) {
	url := c.createURLs("/_api/role", nil)

	var result RoleInfo
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return "", maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return "", maskAny(err)
	}

	return result.Role, nil
}

// Endpoint returns the currently used endpoint for this client.
func (c *client) Endpoint() Endpoint {
	c.endpoints.mutex.RLock()
	defer c.endpoints.mutex.RUnlock()

	return c.endpoints.config
}

// SynchronizeMasterEndpoints ensures that the client is using all known master
// endpoints.
// Do not use for connections to workers.
// Returns true when endpoints have changed.
func (c *client) SynchronizeMasterEndpoints(ctx context.Context) (bool, error) {
	// Fetch all endpoints
	update, err := c.GetEndpoints(ctx)
	if err != nil {
		return false, errors.Wrap(err, "Failed to get master endpoints")
	}
	c.endpoints.mutex.Lock()
	defer c.endpoints.mutex.Unlock()
	if !c.endpoints.config.Equals(update) {
		// Load changed
		list, err := update.URLs()
		if err != nil {
			return false, errors.Wrap(err, "Failed to parse master endpoints")
		}
		c.endpoints.config = update
		c.endpoints.urls = list
		return true, nil
	}
	return false, nil
}

// createURLs creates a full URLs (for all endpoints) for a request with given local path & query.
func (c *client) createURLs(urlPath string, query url.Values) []string {
	c.endpoints.mutex.RLock()
	defer c.endpoints.mutex.RUnlock()

	result := make([]string, len(c.endpoints.urls))
	for i, ep := range c.endpoints.urls {
		u := ep // Create copy
		u.Path = urlPath
		if query != nil {
			u.RawQuery = query.Encode()
		}
		result[i] = u.String()
	}
	return result
}

// newRequests creates new requests with optional body and context
// Returns: request, cancel, error
func (c *client) newRequests(method string, urls []string, body interface{}) ([]*http.Request, error) {
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			return nil, maskAny(err)
		}
	}

	result := make([]*http.Request, len(urls))
	for i, url := range urls {
		var bodyRd io.Reader
		if encoded != nil {
			bodyRd = bytes.NewReader(encoded)
		}
		req, err := http.NewRequest(method, url, bodyRd)
		if err != nil {
			return nil, maskAny(err)
		}
		req.Header.Set(AllowForwardRequestHeaderKey, "true")
		if c.auth.JWTSecret != "" { // nolint: gocritic
			jwt.AddArangoSyncJwtHeader(req, c.auth.JWTSecret)
		} else if c.auth.BearerToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.auth.BearerToken)
		} else if c.auth.UserName != "" {
			plainText := c.auth.UserName + ":" + c.auth.Password
			encoded := base64.StdEncoding.EncodeToString([]byte(plainText))
			req.Header.Set("Authorization", "Basic "+encoded)
		}
		if c.clientID != "" {
			req.Header.Set(ClientIDHeaderKey, c.clientID)
		}
		result[i] = req
	}
	return result, nil
}

type response struct {
	Body       []byte
	StatusCode int
	Request    *http.Request
}

// do performs the given requests all at once.
// The first request to answer with a success or permanent failure is returned.
func (c *client) do(ctx context.Context, reqs []*http.Request, result interface{}, concurrent ...bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel func()
	var timeout time.Duration
	if deadline, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, defaultHTTPTimeout)
		timeout = defaultHTTPTimeout
	} else {
		ctx, cancel = context.WithCancel(ctx)
		timeout = time.Until(deadline)
	}
	defer cancel()

	if len(reqs) > 1 {
		// Shuffle requests to get random distribution
		rand.Shuffle(len(reqs), func(i, j int) {
			reqs[i], reqs[j] = reqs[j], reqs[i]
		})
	}

	if len(concurrent) > 0 && concurrent[0] {
		// All requests concurrently
		if _, err := c.doOnce(ctx, reqs, result); err != nil {
			return maskAny(err)
		}
		return nil
	}

	// Set preferred host first
	for i, req := range reqs {
		if isPreferred, hasPreferred := c.isPreferredHost(req.URL.Host); isPreferred {
			if i == 0 {
				// Already first, we're done
				break
			}
			// Swap with position 0
			reqs[0], reqs[i] = reqs[i], reqs[0]
			break
		} else if !hasPreferred {
			// We have no preferred host, we can stop
			break
		}
	}

	// All requests sequentially
	var lastErr error
	for _, req := range reqs {
		lctx, cancel := context.WithTimeout(ctx, timeout/time.Duration(len(reqs)))
		retryNext, err := c.doOnce(lctx, []*http.Request{req}, result)
		cancel()
		if err == nil {
			return nil
		}
		if retryNext {
			lastErr = err
		} else {
			return maskAny(err)
		}
	}
	if lastErr != nil {
		return maskAny(lastErr)
	}
	return maskAny(errors.Wrapf(ServiceUnavailableError, "No requests available"))
}

// doOnce performs the given requests all at once.
// The first request to answer with a success or permanent failure is returned.
// Return: retryNext, error
func (c *client) doOnce(ctx context.Context, reqs []*http.Request, result interface{}) (bool, error) {
	var cancel context.CancelFunc
	var httpStatusCode int32
	ctx, cancel = context.WithCancel(ctx)
	resultChan := make(chan response, len(reqs))
	errorChan := make(chan error, len(reqs))
	wg := sync.WaitGroup{}

	for regIdx, req := range reqs {
		req = req.WithContext(ctx)
		wg.Add(1)
		go func(regIdx int, req *http.Request) {
			defer wg.Done()

			if len(reqs) > 1 {
				isPreferred, hasPreferred := c.isPreferredHost(req.URL.Host)
				if !isPreferred {
					// Wait a bit depending on our index
					offset := regIdx
					if hasPreferred {
						offset++
					}

					if trigger.WaitWithContext(ctx, time.Microsecond*time.Duration(2*offset)) == trigger.ContextDone {
						errorChan <- maskAny(ctx.Err())
						return
					}
				}
			}
			resp, err := c.client.Do(req)
			if err != nil {
				// Request failed
				errorChan <- maskAny(err)
				return
			}

			// Check status
			statusCode := resp.StatusCode
			if statusCode >= 200 && statusCode < 500 && statusCode != 408 {
				// Read content
				defer resp.Body.Close()
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					errorChan <- maskAny(err)
					return
				}

				// Success or permanent error
				resultChan <- response{
					Body:       body,
					StatusCode: statusCode,
					Request:    req,
				}
				// Cancel all other requests
				cancel()
				return
			}

			atomic.StoreInt32(&httpStatusCode, int32(statusCode))
			// No permanent error, try next agent
		}(regIdx, req)
	}

	// Wait for go routines to finished
	wg.Wait()
	cancel()
	close(resultChan)
	close(errorChan)
	if resp, ok := <-resultChan; ok {
		// Use first valid response
		c.setPreferredHost(resp.Request.URL.Host)

		// Read response body into memory
		if resp.StatusCode != http.StatusOK {
			// Unexpected status, try to parse error.
			return false, maskAny(parseResponseError(resp.Body, resp.StatusCode))
		}

		// Got a success status
		if result != nil {
			if err := json.Unmarshal(resp.Body, result); err != nil {
				method := resp.Request.Method
				url := resp.Request.URL.String()
				return false, errors.Wrapf(err, "Failed decoding response data from %s request to %s: %v", method, url, err)
			}
		}
		return false, nil
	}
	if err, ok := <-errorChan; ok {
		// Return first error
		return false, maskAny(err)
	}

	return true, errors.Wrapf(ServiceUnavailableError,
		"All %d servers responded with temporary failure with http status code: %d", len(reqs), httpStatusCode)
}

// isPreferredHost returns true if the given host is the preferred host to connect to.
// Returns: isGivenHostPreferred, hasPreferredHost
func (c *client) isPreferredHost(host string) (bool, bool) {
	c.endpoints.preferredHostMutex.RLock()
	defer c.endpoints.preferredHostMutex.RUnlock()
	preferredHost := c.endpoints.preferredHost
	if time.Now().After(c.endpoints.preferredHostExpiration) {
		// Our preferred host record has expired.
		// Assume we have no preferred host
		preferredHost = ""
	}
	return host == preferredHost, preferredHost != ""
}

// setPreferredHost records the preferred host to connect to.
func (c *client) setPreferredHost(host string) {
	if isPreferred, _ := c.isPreferredHost(host); !isPreferred {
		c.endpoints.preferredHostMutex.Lock()
		defer c.endpoints.preferredHostMutex.Unlock()
		c.endpoints.preferredHost = host
		c.endpoints.preferredHostExpiration = time.Now().Add(preferredHostTimeout)
	}
}
