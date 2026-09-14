/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package broker

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
)

// GetEdgeClientCredentials mirrors edge-client/pkg/util.GetEdgeClientCredentials (same TLS + host-from-cert OU).
func GetEdgeClientCredentials(certPath, keyPath, caPath string) (credentials.TransportCredentials, string, error) {
	return LoadCredentials(certPath, keyPath, caPath)
}

// LoadCredentials loads TLS credentials and server host from client cert/key/ca (same as edge-client).
// Server host is read from the client cert's Subject.OrganizationalUnit[0] per edge-client util.
func LoadCredentials(certPath, keyPath, caPath string) (credentials.TransportCredentials, string, error) {
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, "", fmt.Errorf("unable to load cert/key: %w", err)
	}
	serverHost, err := getServerHostFromCert(certPath)
	if err != nil {
		return nil, "", err
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, "", fmt.Errorf("unable to read ca cert: %w", err)
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(ca) {
		return nil, "", fmt.Errorf("failed to append ca cert")
	}
	creds := credentials.NewTLS(&tls.Config{
		ServerName:   serverHost,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	})
	return creds, serverHost, nil
}

// GetServerHostFromCert returns the broker hostname from the client certificate OU (same as edge-client).
func GetServerHostFromCert(certPath string) (string, error) {
	return getServerHostFromCert(certPath)
}

func getServerHostFromCert(certPath string) (string, error) {
	pemCert, err := os.ReadFile(certPath)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(pemCert)
	if block == nil {
		return "", fmt.Errorf("invalid cert: no block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", err
	}
	if len(cert.Subject.OrganizationalUnit) > 0 {
		return cert.Subject.OrganizationalUnit[0], nil
	}
	return "", fmt.Errorf("invalid cert: no OU for server host")
}

// EdgeHashIDFromOrganization returns the edge id hash from Subject Organization (O):
// the substring before the first '.', e.g. "7dkgjkx" from "7dkgjkx.cluster.example.com".
func EdgeHashIDFromOrganization(org string) string {
	org = strings.TrimSpace(org)
	if org == "" {
		return ""
	}
	return strings.Split(org, ".")[0]
}

// GetEdgeIDFromClientCert returns the edge id hash from the client certificate Subject Organization (O),
// i.e. strings.Split(O, ".")[0] after trimming. The broker still receives the full TLS peer cert over mTLS;
// this value is used for local logging and optional EDGE_ID checks (same convention as edge hash id elsewhere).
func GetEdgeIDFromClientCert(certPath string) (string, error) {
	pemCert, err := os.ReadFile(certPath)
	if err != nil {
		return "", fmt.Errorf("read client cert: %w", err)
	}
	block, _ := pem.Decode(pemCert)
	if block == nil {
		return "", fmt.Errorf("invalid cert: no PEM block in %s", certPath)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", err
	}
	if len(cert.Subject.Organization) == 0 {
		return "", fmt.Errorf("certificate %s has no Subject Organization (O); edge-broker requires O=<edge-id> (same as edge-client TLS)", certPath)
	}
	o := strings.TrimSpace(cert.Subject.Organization[0])
	if o == "" {
		return "", fmt.Errorf("certificate %s has empty Subject Organization (O)", certPath)
	}
	hash := EdgeHashIDFromOrganization(o)
	if hash == "" {
		return "", fmt.Errorf("certificate %s: Subject O %q yields empty edge id (first DNS label)", certPath, o)
	}
	return hash, nil
}
