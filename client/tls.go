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
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/arangodb-helper/go-certificates"
)

// TLSConfig contains the required parameters to build tls.Config
type TLSConfig struct {
	InsecureSkipVerify bool
	TLSAuth            *TLSAuthentication
}

func (t *TLSConfig) getHash() string {
	if t == nil {
		return "notls"
	}

	parts := []interface{}{
		t.InsecureSkipVerify,
	}
	if t.TLSAuth != nil {
		parts = append(parts,
			t.TLSAuth.ClientKey,
			t.TLSAuth.CACertificate,
			t.TLSAuth.ClientCertificate,
		)
	}
	hashes := make([]string, 0, len(parts))
	for _, p := range parts {
		hash := fmt.Sprintf("%+v", p)
		hash = base64.StdEncoding.EncodeToString([]byte(hash))
		hashes = append(hashes, hash)
	}

	return strings.Join(hashes, "_")
}
func (t *TLSConfig) getTLSConfig() (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}

	if t.TLSAuth != nil {
		return certificates.CreateTLSConfigFromAuthentication(AuthProxy{TLSAuthentication: *t.TLSAuth}, t.InsecureSkipVerify)
	}
	return &tls.Config{
		InsecureSkipVerify: t.InsecureSkipVerify,
	}, nil
}
