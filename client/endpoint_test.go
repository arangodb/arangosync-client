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
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointContains(t *testing.T) {
	ep := Endpoint{"http://a", "http://b", "http://c"}
	for _, x := range []string{"http://a", "http://b", "http://c", "http://a/"} {
		if !ep.Contains(x) {
			t.Errorf("Expected endpoint to contain '%s' but it did not", x)
		}
	}
	for _, x := range []string{"", "http://ab", "-", "http://abc"} {
		if ep.Contains(x) {
			t.Errorf("Expected endpoint to not contain '%s' but it did", x)
		}
	}
}

func TestEndpointIsEmpty(t *testing.T) {
	ep := Endpoint{"http://a", "http://b", "http://c"}
	if ep.IsEmpty() {
		t.Error("Expected endpoint to be not empty, but it is")
	}
	ep = nil
	if !ep.IsEmpty() {
		t.Error("Expected endpoint to be empty, but it is not")
	}
	ep = Endpoint{}
	if !ep.IsEmpty() {
		t.Error("Expected endpoint to be empty, but it is not")
	}
}

func TestEndpointEquals(t *testing.T) {
	expectEqual := []Endpoint{
		{}, {},
		{}, nil,
		{"http://a"}, {"http://a"},
		{"http://a", "http://b"}, {"http://b", "http://a"},
		{"http://foo:8529"}, {"http://foo:8529/"},
	}
	for i := 0; i < len(expectEqual); i += 2 {
		epa := expectEqual[i]
		epb := expectEqual[i+1]
		if !epa.Equals(epb) {
			t.Errorf("Expected endpoint %v to be equal to %v, but it is not", epa, epb)
		}
		if !epb.Equals(epa) {
			t.Errorf("Expected endpoint %v to be equal to %v, but it is not", epb, epa)
		}
	}

	expectNotEqual := []Endpoint{
		{"http://a"}, {},
		{"http://z"}, nil,
		{"http://aa"}, {"http://a"},
		{"http://a:100"}, {"http://a:200"},
		{"http://a", "http://b", "http://c"}, {"http://b", "http://a"},
	}
	for i := 0; i < len(expectNotEqual); i += 2 {
		epa := expectNotEqual[i]
		epb := expectNotEqual[i+1]
		if epa.Equals(epb) {
			t.Errorf("Expected endpoint %v to be not equal to %v, but it is", epa, epb)
		}
		if epb.Equals(epa) {
			t.Errorf("Expected endpoint %v to be not equal to %v, but it is", epb, epa)
		}
	}
}

func TestEndpointClone(t *testing.T) {
	tests := []Endpoint{
		{},
		{"http://a"},
		{"http://a", "http://b"},
	}
	for _, orig := range tests {
		c := orig.Clone()
		if !orig.Equals(c) {
			t.Errorf("Expected endpoint %v to be equal to clone %v, but it is not", orig, c)
		}
		if len(c) > 0 {
			c[0] = "http://modified"
			if orig.Equals(c) {
				t.Errorf("Expected endpoint %v to be no longer equal to clone %v, but it is", orig, c)
			}
		}
	}
}

func TestEndpointIntersection(t *testing.T) {
	expectIntersection := []Endpoint{
		{"http://a"}, {"http://a"},
		{"http://a"}, {"http://a", "http://b"},
		{"http://a"}, {"http://b", "http://a"},
		{"http://a", "http://b"}, {"http://b", "http://foo27"},
		{"http://foo:8529"}, {"http://foo:8529/"},
	}
	for i := 0; i < len(expectIntersection); i += 2 {
		epa := expectIntersection[i]
		epb := expectIntersection[i+1]
		if len(epa.Intersection(epb)) == 0 {
			t.Errorf("Expected endpoint %v to have an intersection with %v, but it does not", epa, epb)
		}
		if len(epb.Intersection(epa)) == 0 {
			t.Errorf("Expected endpoint %v to have an intersection with %v, but it does not", epb, epa)
		}
	}

	expectNoIntersection := []Endpoint{
		{"http://a"}, {},
		{"http://z"}, nil,
		{"http://aa"}, {"http://a"},
		{"http://a", "http://b", "http://c"}, {"http://e", "http://f"},
	}
	for i := 0; i < len(expectNoIntersection); i += 2 {
		epa := expectNoIntersection[i]
		epb := expectNoIntersection[i+1]
		if len(epa.Intersection(epb)) > 0 {
			t.Errorf("Expected endpoint %v to have no intersection with %v, but it does", epa, epb)
		}
		if len(epb.Intersection(epa)) > 0 {
			t.Errorf("Expected endpoint %v to havenoan intersection with %v, but it does", epb, epa)
		}
	}
}

func TestEndpointValidate(t *testing.T) {
	validTests := []Endpoint{
		{},
		{"http://a"},
		{"http://a", "http://b"},
	}
	for _, x := range validTests {
		if err := x.Validate(); err != nil {
			t.Errorf("Expected endpoint %v to be valid, but it is not because %s", x, err)
		}
	}
	invalidTests := []Endpoint{
		{":http::foo"},
		{"http/a"},
		{"http??"},
		{"http:/"},
		{"http:/foo"},
	}
	for _, x := range invalidTests {
		if err := x.Validate(); err == nil {
			t.Errorf("Expected endpoint %v to be not valid, but it is", x)
		}
	}
}

func TestEndpointURLs(t *testing.T) {
	ep := Endpoint{"http://a", "http://b/rel"}
	expected := []string{"http://a", "http://b"}
	list, err := ep.URLs()
	if err != nil {
		t.Errorf("URLs expected to succeed, but got %s", err)
	} else {
		for i, x := range list {
			found := x.String()
			if found != expected[i] {
				t.Errorf("Unexpected URL at index %d of %v, expected '%s', got '%s'", i, ep, expected[i], found)
			}
		}
	}
}

func TestEndpointMerge(t *testing.T) {
	tests := []Endpoint{
		{"http://a"}, {}, {"http://a"},
		{"http://z"}, nil, {"http://z"},
		{"http://aa"}, {"http://a"}, {"http://aa", "http://a"},
		{"http://a", "http://b", "http://c"}, {"http://e", "http://f"}, {"http://a", "http://b", "http://c", "http://e", "http://f"},
		{"http://a", "http://b", "http://c"}, {"http://a", "http://f"}, {"http://a", "http://b", "http://c", "http://f"},
	}
	for i := 0; i < len(tests); i += 3 {
		epa := tests[i]
		epb := tests[i+1]
		expected := tests[i+2]
		result := epa.Merge(epb...)
		if !result.Equals(expected) {
			t.Errorf("Expected merge of endpoints %v & %v to be %v, but got %v", epa, epb, expected, result)
		}
	}
}

func TestEndpoint_EqualsOrder(t *testing.T) {
	tests := map[string]struct {
		source Endpoint
		target Endpoint
		want   bool
	}{
		"nil source to and nil target": {
			want: true,
		},
		"nil source to and not nil target": {
			target: Endpoint{"1"},
		},
		"The source and target are the same": {
			source: Endpoint{"1", "2"},
			target: Endpoint{"1", "2"},
			want:   true,
		},
		"The order of the source and target is not the same": {
			source: Endpoint{"2", "1"},
			target: Endpoint{"1", "2"},
		},
		"The number of endpoints of the source and target is not the same": {
			source: Endpoint{"1", "2", "3"},
			target: Endpoint{"1", "2"},
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			got := tt.source.EqualsOrder(tt.target)
			assert.Equalf(t, tt.want, got, "")
		})
	}
}

func TestParseEndpoint(t *testing.T) {
	type args struct {
		endpoint      string
		fixupEndpoint bool
	}
	tests := map[string]struct {
		args    args
		want    *url.URL
		wantErr error
	}{
		"test": {
			args: args{
				endpoint: "",
			},
			wantErr: ErrEmptyValue,
		},
		"empty hostname": {
			args: args{
				endpoint: "http://:8529",
			},
			wantErr: errors.New("the endpoint \"http://:8529\" is missing a hostname"),
		},
		"valid hostname": {
			args: args{
				endpoint: "http://localhost:8529",
			},
			want: &url.URL{
				Scheme: "http",
				Host:   "localhost:8529",
			},
		},
		"fixup endpoint scheme": {
			args: args{
				endpoint:      "ssl://localhost:8529",
				fixupEndpoint: true,
			},
			want: &url.URL{
				Scheme: "https",
				Host:   "localhost:8529",
			},
		},
		"normalized endpoint": {
			args: args{
				endpoint: "postgres://example.com:5432/db?sslmode=require",
			},
			want: &url.URL{
				Scheme: "postgres",
				Host:   "example.com:5432",
			},
			wantErr: nil,
		},
		"invalid endpoint": {
			args: args{
				endpoint: "http://test.com/Segment%%2815197306101420000%29.ts",
			},
			wantErr: errors.New("invalid URL escape \"%%2\""),
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			got, err := ParseEndpoint(testCase.args.endpoint, testCase.args.fixupEndpoint)
			if testCase.wantErr != nil {
				require.Error(t, testCase.wantErr, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCase.want, got)
		})
	}
}
