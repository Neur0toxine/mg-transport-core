package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/retailcrm/mg-transport-core/v2/core/logger"
	"go.uber.org/zap"
)

type Auth struct {
	Username        string
	Password        string
	Token           string
	CredentialsFile string
	NKeySeedFile    string
	UserJWT         string
	JWTSeedFile     string
}

type Config struct {
	URLs                 []string
	Name                 string
	Auth                 Auth
	TLS                  *tls.Config
	ConnectTimeout       time.Duration
	ReconnectWait        time.Duration
	DrainTimeout         time.Duration
	MaxReconnects        int
	RetryOnFailedConnect bool
}

type Client struct {
	Conn      *natsgo.Conn
	JetStream jetstream.JetStream
}

func Connect(ctx context.Context, config Config, log logger.Logger, additional ...natsgo.Option) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if log == nil {
		log = logger.NewNil()
	}
	if len(config.URLs) == 0 {
		config.URLs = []string{natsgo.DefaultURL}
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 2 * time.Second
	}
	if config.ReconnectWait <= 0 {
		config.ReconnectWait = 2 * time.Second
	}
	if config.DrainTimeout <= 0 {
		config.DrainTimeout = 30 * time.Second
	}
	if config.MaxReconnects == 0 {
		config.MaxReconnects = 60
	}

	options := []natsgo.Option{
		natsgo.Name(config.Name), natsgo.Timeout(config.ConnectTimeout), natsgo.ReconnectWait(config.ReconnectWait),
		natsgo.MaxReconnects(config.MaxReconnects), natsgo.RetryOnFailedConnect(config.RetryOnFailedConnect),
		natsgo.DrainTimeout(config.DrainTimeout),
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) { log.Warn("NATS disconnected", zap.Error(err)) }),
		natsgo.ReconnectHandler(func(connection *natsgo.Conn) {
			log.Info("NATS reconnected", zap.String("url", connection.ConnectedUrl()))
		}),
		natsgo.ReconnectErrHandler(func(_ *natsgo.Conn, err error) { log.Warn("NATS reconnect failed", zap.Error(err)) }),
		natsgo.ClosedHandler(func(connection *natsgo.Conn) { log.Info("NATS connection closed", zap.Error(connection.LastError())) }),
	}
	auth, err := authOption(config.Auth)
	if err != nil {
		return nil, err
	}
	if auth != nil {
		options = append(options, auth)
	}
	if config.TLS != nil {
		options = append(options, natsgo.Secure(config.TLS.Clone()))
	}
	options = append(options, additional...)

	connection, err := natsgo.Connect(strings.Join(config.URLs, ","), options...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("create JetStream client: %w", err)
	}
	return &Client{Conn: connection, JetStream: js}, nil
}

func authOption(auth Auth) (natsgo.Option, error) {
	methods := 0
	if auth.Username != "" || auth.Password != "" {
		methods++
	}
	if auth.Token != "" {
		methods++
	}
	if auth.CredentialsFile != "" {
		methods++
	}
	if auth.NKeySeedFile != "" {
		methods++
	}
	if auth.UserJWT != "" || auth.JWTSeedFile != "" {
		methods++
	}
	if methods > 1 {
		return nil, errors.New("configure exactly one NATS authentication method")
	}
	if auth.Password != "" && auth.Username == "" {
		return nil, errors.New("NATS password requires a username")
	}
	if (auth.UserJWT == "") != (auth.JWTSeedFile == "") {
		return nil, errors.New("NATS JWT authentication requires both JWT and seed file")
	}
	switch {
	case auth.Username != "":
		return natsgo.UserInfo(auth.Username, auth.Password), nil
	case auth.Token != "":
		return natsgo.Token(auth.Token), nil
	case auth.CredentialsFile != "":
		return natsgo.UserCredentials(auth.CredentialsFile), nil
	case auth.NKeySeedFile != "":
		return natsgo.NkeyOptionFromSeed(auth.NKeySeedFile)
	case auth.UserJWT != "":
		return natsgo.UserJWTAndSeed(auth.UserJWT, auth.JWTSeedFile), nil
	default:
		return nil, nil
	}
}

func (c *Client) Drain(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- c.Conn.Drain() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		c.Conn.Close()
		return ctx.Err()
	}
}

func (c *Client) Close() { c.Conn.Close() }
