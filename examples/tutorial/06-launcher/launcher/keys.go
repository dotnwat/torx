//go:build unix

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// The key files, all under keysDir in the run directory. The node image
// bakes in host_key and authorized_keys; the suite dials with client_key
// and verifies the nodes against known_hosts.
const (
	clientKeyFile      = "client_key"
	hostKeyFile        = "host_key"
	authorizedKeysFile = "authorized_keys"
	knownHostsFile     = "known_hosts"
)

// writeKeys generates a fresh client key pair and host key pair into dir and
// derives the two files that connect them: the authorized_keys the nodes
// accept the client key from, and the known_hosts the client verifies every
// node's host key against. Generating both sides here, before any
// container exists, means the suite never has to trust a host key on first
// use, which the torx ssh backend could not do anyway, and a run's keys are
// its own.
func writeKeys(dir string, hosts []string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	clientPub, err := writeKeyPair(filepath.Join(dir, clientKeyFile))
	if err != nil {
		return err
	}
	hostPub, err := writeKeyPair(filepath.Join(dir, hostKeyFile))
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, authorizedKeysFile), ssh.MarshalAuthorizedKey(clientPub), 0o644); err != nil {
		return err
	}
	line := knownhosts.Line(hosts, hostPub) + "\n"
	return os.WriteFile(filepath.Join(dir, knownHostsFile), []byte(line), 0o644)
}

// writeKeyPair writes a new ed25519 private key to path in OpenSSH format,
// readable only by its owner, and its public key beside it as path.pub.
func writeKeyPair(path string) (ssh.PublicKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, fmt.Errorf("marshal %s: %w", path, err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		return nil, err
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path+".pub", ssh.MarshalAuthorizedKey(sshPub), 0o644); err != nil {
		return nil, err
	}
	return sshPub, nil
}
