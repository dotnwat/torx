//go:build unix

package ssh

import (
	"fmt"
	"os"
	"time"

	cryptossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/dotnwat/torx"
)

// defaultPort is the SSH port assumed when a Config does not set one.
const defaultPort = 22

// Config is the ssh-kind BackendDescriptor.Config: the per-node connection
// settings the driver serializes and the worker uses to dial. Authentication is
// public-key only, and the host key is always verified against KnownHosts -- a
// remote endpoint is untrusted, so there is no insecure fallback.
type Config struct {
	Port             int    `json:"port,omitempty"`               // TCP port; 0 means defaultPort
	User             string `json:"user"`                         // login user
	IdentityFile     string `json:"identity_file"`                // PEM private key on the worker host
	KnownHosts       string `json:"known_hosts,omitempty"`        // known_hosts file for host-key verification
	ConnectTimeoutMS int    `json:"connect_timeout_ms,omitempty"` // SSH handshake timeout; 0 means none
}

// clientConfig builds the crypto/ssh client configuration from cfg, resolving
// the identity key and host-key callback up front so a misconfiguration fails
// before any connection is attempted.
func clientConfig(cfg Config) (*cryptossh.ClientConfig, error) {
	if cfg.User == "" {
		return nil, fmt.Errorf("ssh: config requires a user")
	}
	if cfg.IdentityFile == "" {
		return nil, fmt.Errorf("ssh: config requires an identity_file")
	}
	if cfg.KnownHosts == "" {
		return nil, fmt.Errorf("ssh: config requires known_hosts for host-key verification")
	}
	pemBytes, err := os.ReadFile(cfg.IdentityFile)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: read identity file", err)
	}
	signer, err := cryptossh.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: parse identity file", err)
	}
	hostKeys, err := knownhosts.New(cfg.KnownHosts)
	if err != nil {
		return nil, torx.Wrap(torx.ErrBackend, "ssh: read known_hosts", err)
	}
	return &cryptossh.ClientConfig{
		User:            cfg.User,
		Auth:            []cryptossh.AuthMethod{cryptossh.PublicKeys(signer)},
		HostKeyCallback: hostKeys,
		Timeout:         time.Duration(cfg.ConnectTimeoutMS) * time.Millisecond,
	}, nil
}
