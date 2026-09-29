/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/credentials/insecure"
)

// msCA is a self-signed test CA.
type msCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func msNewCA(t *testing.T) *msCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ms-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return &msCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// msClientCert issues a client certificate with the given Subject and returns PEM cert and key.
func (ca *msCA) msClientCert(t *testing.T, subject pkix.Name) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// msWriteFile writes content into dir/name and returns the path.
func msWriteFile(t *testing.T, dir, name string, content []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

// msCertDir lays out client.crt / client.key / ca.crt the way the controller's CERT_FOLDER does.
func msCertDir(t *testing.T, ca *msCA, subject pkix.Name) (certPath, keyPath, caPath string) {
	t.Helper()
	dir := t.TempDir()
	certPEM, keyPEM := ca.msClientCert(t, subject)
	return msWriteFile(t, dir, "client.crt", certPEM),
		msWriteFile(t, dir, "client.key", keyPEM),
		msWriteFile(t, dir, "ca.crt", ca.pem)
}

var msEdgeSubject = pkix.Name{
	CommonName:         "edge-client",
	Organization:       []string{"7dkgjkx.cluster.example.com"},
	OrganizationalUnit: []string{"broker.example.com"},
}

func TestMsLoadCredentials(t *testing.T) {
	ca := msNewCA(t)
	certPath, keyPath, caPath := msCertDir(t, ca, msEdgeSubject)

	creds, host, err := LoadCredentials(certPath, keyPath, caPath)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}
	if host != "broker.example.com" {
		t.Errorf("server host must come from the client cert OU, got %q", host)
	}
	if creds == nil {
		t.Fatal("nil credentials")
	}
	if info := creds.Info(); info.SecurityProtocol != "tls" || info.ServerName != "broker.example.com" {
		t.Errorf("TLS ServerName must be the OU host: %+v", info)
	}

	// GetEdgeClientCredentials is the edge-client-named alias of the same loader.
	creds2, host2, err := GetEdgeClientCredentials(certPath, keyPath, caPath)
	if err != nil || creds2 == nil || host2 != host {
		t.Errorf("GetEdgeClientCredentials: creds=%v host=%q err=%v", creds2 != nil, host2, err)
	}
}

func TestMsLoadCredentialsErrors(t *testing.T) {
	ca := msNewCA(t)
	certPath, keyPath, caPath := msCertDir(t, ca, msEdgeSubject)
	dir := t.TempDir()

	t.Run("missing key", func(t *testing.T) {
		_, _, err := LoadCredentials(certPath, filepath.Join(dir, "absent.key"), caPath)
		if err == nil || !strings.Contains(err.Error(), "unable to load cert/key") {
			t.Errorf("want cert/key load error, got %v", err)
		}
	})
	t.Run("key does not match cert", func(t *testing.T) {
		_, otherKey := ca.msClientCert(t, msEdgeSubject)
		otherKeyPath := msWriteFile(t, dir, "other.key", otherKey)
		_, _, err := LoadCredentials(certPath, otherKeyPath, caPath)
		if err == nil || !strings.Contains(err.Error(), "unable to load cert/key") {
			t.Errorf("want cert/key mismatch surfaced as a load error, got %v", err)
		}
	})
	t.Run("cert without OU", func(t *testing.T) {
		noOU := msEdgeSubject
		noOU.OrganizationalUnit = nil
		c, k, a := msCertDir(t, ca, noOU)
		_, _, err := LoadCredentials(c, k, a)
		if err == nil || !strings.Contains(err.Error(), "no OU") {
			t.Errorf("a cert without OU has no broker host to dial, got %v", err)
		}
	})
	t.Run("missing ca", func(t *testing.T) {
		_, _, err := LoadCredentials(certPath, keyPath, filepath.Join(dir, "absent-ca.crt"))
		if err == nil || !strings.Contains(err.Error(), "unable to read ca cert") {
			t.Errorf("want ca read error, got %v", err)
		}
	})
	t.Run("ca without a certificate", func(t *testing.T) {
		badCA := msWriteFile(t, dir, "bad-ca.crt", []byte("not a pem file"))
		_, _, err := LoadCredentials(certPath, keyPath, badCA)
		if err == nil || !strings.Contains(err.Error(), "failed to append ca cert") {
			t.Errorf("want ca append error, got %v", err)
		}
	})
}

func TestMsGetServerHostFromCert(t *testing.T) {
	ca := msNewCA(t)
	dir := t.TempDir()

	t.Run("first OU wins", func(t *testing.T) {
		subj := msEdgeSubject
		subj.OrganizationalUnit = []string{"first.example.com", "second.example.com"}
		certPath, _, _ := msCertDir(t, ca, subj)
		host, err := GetServerHostFromCert(certPath)
		if err != nil || host != "first.example.com" {
			t.Errorf("got (%q, %v)", host, err)
		}
	})
	t.Run("no OU", func(t *testing.T) {
		subj := msEdgeSubject
		subj.OrganizationalUnit = nil
		certPath, _, _ := msCertDir(t, ca, subj)
		if _, err := GetServerHostFromCert(certPath); err == nil || !strings.Contains(err.Error(), "no OU") {
			t.Errorf("want no-OU error, got %v", err)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		if _, err := GetServerHostFromCert(filepath.Join(dir, "absent.crt")); err == nil {
			t.Error("want read error")
		}
	})
	t.Run("no PEM block", func(t *testing.T) {
		p := msWriteFile(t, dir, "plain.crt", []byte("hello"))
		if _, err := GetServerHostFromCert(p); err == nil || !strings.Contains(err.Error(), "no block") {
			t.Errorf("want no-block error, got %v", err)
		}
	})
	t.Run("PEM block with garbage DER", func(t *testing.T) {
		p := msWriteFile(t, dir, "garbage.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{1, 2, 3}}))
		if _, err := GetServerHostFromCert(p); err == nil {
			t.Error("want x509 parse error")
		}
	})
}

func TestMsGetEdgeIDFromClientCert(t *testing.T) {
	ca := msNewCA(t)
	dir := t.TempDir()

	t.Run("first DNS label of O", func(t *testing.T) {
		certPath, _, _ := msCertDir(t, ca, msEdgeSubject)
		id, err := GetEdgeIDFromClientCert(certPath)
		if err != nil || id != "7dkgjkx" {
			t.Errorf("got (%q, %v), want 7dkgjkx", id, err)
		}
	})
	t.Run("O without dots is used whole", func(t *testing.T) {
		subj := msEdgeSubject
		subj.Organization = []string{"  edgehash  "}
		certPath, _, _ := msCertDir(t, ca, subj)
		id, err := GetEdgeIDFromClientCert(certPath)
		if err != nil || id != "edgehash" {
			t.Errorf("got (%q, %v), want trimmed O", id, err)
		}
	})
	t.Run("no O", func(t *testing.T) {
		subj := msEdgeSubject
		subj.Organization = nil
		certPath, _, _ := msCertDir(t, ca, subj)
		_, err := GetEdgeIDFromClientCert(certPath)
		if err == nil || !strings.Contains(err.Error(), "no Subject Organization") || !strings.Contains(err.Error(), certPath) {
			t.Errorf("want no-O error naming the file, got %v", err)
		}
	})
	t.Run("blank O", func(t *testing.T) {
		subj := msEdgeSubject
		subj.Organization = []string{"   "}
		certPath, _, _ := msCertDir(t, ca, subj)
		_, err := GetEdgeIDFromClientCert(certPath)
		if err == nil || !strings.Contains(err.Error(), "empty Subject Organization") {
			t.Errorf("want empty-O error, got %v", err)
		}
	})
	t.Run("O whose first label is empty", func(t *testing.T) {
		subj := msEdgeSubject
		subj.Organization = []string{".cluster.example.com"}
		certPath, _, _ := msCertDir(t, ca, subj)
		_, err := GetEdgeIDFromClientCert(certPath)
		if err == nil || !strings.Contains(err.Error(), "yields empty edge id") {
			t.Errorf("want empty-label error, got %v", err)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := GetEdgeIDFromClientCert(filepath.Join(dir, "absent.crt"))
		if err == nil || !strings.Contains(err.Error(), "read client cert") {
			t.Errorf("want read error, got %v", err)
		}
	})
	t.Run("no PEM block", func(t *testing.T) {
		p := msWriteFile(t, dir, "plain.crt", []byte("hello"))
		_, err := GetEdgeIDFromClientCert(p)
		if err == nil || !strings.Contains(err.Error(), "no PEM block") {
			t.Errorf("want no-PEM error, got %v", err)
		}
	})
	t.Run("PEM block with garbage DER", func(t *testing.T) {
		p := msWriteFile(t, dir, "garbage.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{1, 2, 3}}))
		if _, err := GetEdgeIDFromClientCert(p); err == nil {
			t.Error("want x509 parse error")
		}
	})
}

func TestMsEdgeHashIDFromOrganization(t *testing.T) {
	for in, want := range map[string]string{
		"":                            "",
		"   ":                         "",
		"7dkgjkx":                     "7dkgjkx",
		"7dkgjkx.cluster.example.com": "7dkgjkx",
		"  7dkgjkx.example.com \n":    "7dkgjkx",
		".example.com":                "",
		"a.b":                         "a",
	} {
		if got := EdgeHashIDFromOrganization(in); got != want {
			t.Errorf("EdgeHashIDFromOrganization(%q) = %q, want %q", in, got, want)
		}
	}
}

// NewSessionID must produce a fresh random UUID v4 per call, matching edge-common's NewID.
func TestMsNewSessionID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		id := NewSessionID()
		u, err := uuid.Parse(id)
		if err != nil {
			t.Fatalf("session id %q is not a UUID: %v", id, err)
		}
		if u.Version() != 4 {
			t.Fatalf("session id %q is UUID v%d, want v4", id, u.Version())
		}
		if seen[id] {
			t.Fatalf("session id %q repeated", id)
		}
		seen[id] = true
	}
}

// The dial helpers build the address as host:port and connect lazily, so constructing a
// connection to an unreachable broker must not block or fail.
func TestMsDialHelpersAreLazy(t *testing.T) {
	ctx := context.Background()

	insecureConn, err := NewInsecureGrpcClientConn(ctx, "127.0.0.1", 5449)
	if err != nil {
		t.Fatalf("NewInsecureGrpcClientConn: %v", err)
	}
	defer insecureConn.Close()
	if got := insecureConn.Target(); got != "127.0.0.1:5449" {
		t.Errorf("insecure target: %q", got)
	}

	ca := msNewCA(t)
	certPath, keyPath, caPath := msCertDir(t, ca, msEdgeSubject)
	creds, host, err := LoadCredentials(certPath, keyPath, caPath)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}
	secureConn, err := NewSecureGrpcClientConn(ctx, host, 5448, creds)
	if err != nil {
		t.Fatalf("NewSecureGrpcClientConn: %v", err)
	}
	defer secureConn.Close()
	if got := secureConn.Target(); got != "broker.example.com:5448" {
		t.Errorf("secure target: %q", got)
	}

	dialConn, err := Dial(ctx, "localhost", 1, insecure.NewCredentials())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer dialConn.Close()
	if got := dialConn.Target(); got != "localhost:1" {
		t.Errorf("Dial target: %q", got)
	}
}
