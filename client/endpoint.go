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
	"fmt"
	"net/url"
	"sort"

	"github.com/pkg/errors"

	"github.com/arangodb/go-driver/util"
)

var ErrEmptyValue = errors.New("empty value")

// Endpoint is a list of URLs that are considered to be of the same service.
type Endpoint []string

// EndpointsCreator describes how endpoints are used for.
type EndpointsCreator interface {
	// GetEndpoints returns endpoints for the connection.
	GetEndpoints() Endpoint
	// IsInternal returns true if endpoints are in the same datacenter.
	IsInternal() bool
}

type endpoints struct {
	Endpoint
}

// GetEndpoints returns all endpoints for the connection.
func (i endpoints) GetEndpoints() Endpoint {
	return i.Endpoint
}

// InternalEndpoints describes endpoints to the internal datacenter.
type InternalEndpoints struct {
	endpoints
}

// ExternalEndpoints describes endpoints to the external datacenter.
type ExternalEndpoints struct {
	endpoints
}

// IsInternal return true for the internal endpoints.
func (i InternalEndpoints) IsInternal() bool {
	return true
}

// IsInternal return false for the external endpoints.
func (i ExternalEndpoints) IsInternal() bool {
	return false
}

// NewInternalEndpoints creates a new list of endpoints to the internal DC's arangosync server (master, worker).
func NewInternalEndpoints(e Endpoint) *InternalEndpoints {
	return &InternalEndpoints{
		endpoints: endpoints{
			e,
		},
	}
}

// NewExternalEndpoints creates a new list of endpoints to the external DC's arangosync server (master, worker).
func NewExternalEndpoints(e Endpoint) *ExternalEndpoints {
	return &ExternalEndpoints{
		endpoints{
			e,
		},
	}
}

// Contains returns true when x is an element of ep.
func (ep Endpoint) Contains(x string) bool {
	x = normalizeSingleEndpoint(x)
	for _, y := range ep {
		if x == normalizeSingleEndpoint(y) {
			return true
		}
	}
	return false
}

// IsEmpty returns true ep has no elements.
func (ep Endpoint) IsEmpty() bool {
	return len(ep) == 0
}

// Clone returns a deep clone of the given endpoint
func (ep Endpoint) Clone() Endpoint {
	return append(Endpoint{}, ep...)
}

// Equals returns true when a and b contain
// the same elements (perhaps in different order).
func (ep Endpoint) Equals(other Endpoint) bool {
	if len(ep) != len(other) {
		return false
	}
	// Clone lists so we can sort them without affecting the original lists.
	a := append([]string{}, ep.normalized()...)
	b := append([]string{}, other.normalized()...)
	sort.Strings(a)
	sort.Strings(b)
	for i, x := range a {
		if x != b[i] {
			return false
		}
	}
	return true
}

// Intersection the endpoint containing all elements included in ep and in other.
func (ep Endpoint) Intersection(other Endpoint) Endpoint {
	result := make([]string, 0, len(ep)+len(other))
	for _, x := range ep {
		if other.Contains(x) {
			result = append(result, x)
		}
	}
	sort.Strings(result)
	return result
}

// EqualsOrder returns true if endpoints are the same including order.
func (ep Endpoint) EqualsOrder(other Endpoint) bool {
	if len(ep) != len(other) {
		return false
	}

	for i := range ep {
		if ep[i] != other[i] {
			return false
		}
	}

	return true
}

// Validate checks all URL's, returning the first error found.
func (ep Endpoint) Validate() error {
	for _, x := range ep {
		if u, err := url.Parse(x); err != nil {
			return maskAny(fmt.Errorf("Endpoint '%s' is invalid: %s", x, err.Error()))
		} else if u.Host == "" {
			return maskAny(fmt.Errorf("Endpoint '%s' is missing a host", x))
		}
	}
	return nil
}

// URLs returns all endpoints as parsed URL's
func (ep Endpoint) URLs() ([]url.URL, error) {
	list := make([]url.URL, 0, len(ep))
	for _, x := range ep {
		u, err := url.Parse(x)
		if err != nil {
			return nil, errors.Wrapf(err, "can not parse the URL %s", x)
		}
		u.Path = ""
		list = append(list, *u)
	}
	return list, nil
}

// Merge adds the given endpoint to the endpoint, avoiding duplicates
func (ep Endpoint) Merge(args ...string) Endpoint {
	m := make(map[string]struct{})
	for _, x := range ep {
		m[x] = struct{}{}
	}
	for _, x := range args {
		m[x] = struct{}{}
	}
	result := make([]string, 0, len(m))
	for x := range m {
		result = append(result, x)
	}
	sort.Strings(result)
	return result
}

// normalized returns a clone of the given endpoint that contains normalized elements
func (ep Endpoint) normalized() Endpoint {
	result := make(Endpoint, len(ep))
	for i, x := range ep {
		result[i] = normalizeSingleEndpoint(x)
	}
	return result
}

func normalizeSingleEndpoint(ep string) string {
	if u, err := url.Parse(ep); err == nil {
		u.Path = ""
		return u.String()
	}
	return ep
}

// ParseEndpoint returns parsed URL if hostname is set.
// The URL is normalized.
func ParseEndpoint(endpoint string, fixupEndpoint bool) (*url.URL, error) {
	if len(endpoint) == 0 {
		return nil, ErrEmptyValue
	}

	if fixupEndpoint {
		endpoint = util.FixupEndpointURLScheme(endpoint)
	}

	url, err := url.Parse(endpoint)
	if err == nil && len(url.Hostname()) > 0 {
		NormalizeEndpoint(url)

		return url, nil
	}

	if err != nil {
		return nil, errors.Wrapf(err, "the endpoint \"%s\" is invalid", endpoint)
	}

	return nil, fmt.Errorf("the endpoint \"%s\" is missing a hostname", endpoint)
}

// NormalizeEndpoint cuts off everything what is after the host:port
func NormalizeEndpoint(url *url.URL) {
	if url != nil {
		url.Path, url.RawQuery, url.Fragment, url.ForceQuery = "", "", "", false
	}
}
