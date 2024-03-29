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
	"crypto/sha1"
	"fmt"
	"strings"
	"sync"

	"github.com/arangodb/arangosync-client/pkg/errors"
)

type Cacher interface {
	GetClient(source EndpointsCreator, auth Authentication, insecureSkipVerify bool) (API, error)
}

type ClientCache struct {
	mutex   sync.Mutex
	clients map[string]API
}

// GetClient returns a client used to access the server with given authentication.
func (cc *ClientCache) GetClient(endpointsCreator EndpointsCreator, auth Authentication, insecureSkipVerify bool) (API, error) {
	endpoints := endpointsCreator.GetEndpoints()
	if len(endpoints) == 0 {
		return nil, errors.WithMessage(PreconditionFailedError, "no endpoints configured")
	}

	keyData := strings.Join(endpoints, ",") + ":" + auth.String()
	key := fmt.Sprintf("%x", sha1.Sum([]byte(keyData)))

	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	if cc.clients == nil {
		cc.clients = make(map[string]API)
	}

	// Get existing client (if any)
	if c, ok := cc.clients[key]; ok {
		return c, nil
	}

	c, err := cc.createClient(endpoints, endpointsCreator.IsInternal(), auth, insecureSkipVerify)
	if err != nil {
		return nil, errors.WithMessage(err, "can not create client")
	}

	cc.clients[key] = c
	c.SetShared()
	return c, nil
}

// createClient creates a client used to access the source with given authentication.
func (cc *ClientCache) createClient(endpoints Endpoint, isInternal bool, auth Authentication, insecureSkipVerify bool) (API, error) {
	tlsConfig := &TLSConfig{
		InsecureSkipVerify: insecureSkipVerify,
		TLSAuth:            &auth.TLSAuthentication,
	}

	ac := createAuthenticationConfig(auth)
	c, err := NewArangoSyncClient(endpoints, isInternal, ac, tlsConfig)
	if err != nil {
		if isInternal {
			return nil, errors.WithMessage(err, "can not create internal arangosync client")
		}

		return nil, errors.WithMessage(err, "can not create external arangosync client")
	}
	return c, nil
}

// NewAuthentication creates a new Authentication from given arguments.
func NewAuthentication(tlsAuth TLSAuthentication, jwtSecret string) Authentication {
	return Authentication{
		TLSAuthentication: tlsAuth,
		JWTSecret:         jwtSecret,
	}
}

// Authentication contains all possible authentication methods for a client.
// Order of authentication methods:
// - JWTSecret
// - ClientToken
// - ClientCertificate
type Authentication struct {
	TLSAuthentication
	JWTSecret string
	Username  string
	Password  string
}

// String returns a string used to unique identify the authentication settings.
func (a Authentication) String() string {
	return a.TLSAuthentication.String() + ":" + a.JWTSecret + ":" + a.Username + ":" + a.Password
}

// AuthProxy is a helper that implements github.com/arangodb-helper/go-certificates#TLSAuthentication.
type AuthProxy struct {
	TLSAuthentication
}

func (a AuthProxy) CACertificate() string     { return a.TLSAuthentication.CACertificate }
func (a AuthProxy) ClientCertificate() string { return a.TLSAuthentication.ClientCertificate }
func (a AuthProxy) ClientKey() string         { return a.TLSAuthentication.ClientKey }

func createAuthenticationConfig(auth Authentication) AuthenticationConfig {
	ac := AuthenticationConfig{}
	if auth.Username != "" {
		ac.UserName = auth.Username
		ac.Password = auth.Password

		return ac
	}

	if auth.JWTSecret != "" {
		ac.JWTSecret = auth.JWTSecret
		return ac
	}

	ac.BearerToken = auth.ClientToken
	return ac
}
