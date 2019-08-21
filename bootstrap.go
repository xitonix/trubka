package main

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/gookit/color"
	"github.com/pkg/errors"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

var (
	profilingMode           string
	protoDir                string
	logFilePath             string
	outputDir               string
	topic                   string
	messageType             string
	format                  string
	kafkaVersion            string
	environment             string
	topicFilter             string
	typeFilter              string
	searchQuery             string
	saslUsername            string
	saslPassword            string
	saslMechanism           string
	tlsCACert               string
	tlsClientKey            string
	tlsClientCert           string
	terminalMode            string
	brokers                 []string
	protoFiles              []string
	interactive             bool
	reverse                 bool
	includeTimeStamp        bool
	rewind                  bool
	enableTLS               bool
	enableAutoTopicCreation bool
	versionRequest          bool
	timeCheckpoint          time.Time
	offsetCheckpoint        int64
	verbosity               int
)

func getLogWriter(f string) (io.Writer, bool, error) {
	file := f.Get()
	if internal.IsEmpty(file) {
		if f.IsSet() {
			return ioutil.Discard, false, nil
		}
		return os.Stdout, false, nil
	}
	lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
	}
	return lf, true, nil
}

func getOutputWriters(dir string, topics map[string]*kafka.Checkpoint) (map[string]io.Writer, bool, error) {
	root := dir.Get()
	result := make(map[string]io.Writer)

	if internal.IsEmpty(root) {
		discard := dir.IsSet()
		for topic := range topics {
			if discard {
				result[topic] = ioutil.Discard
				continue
			}
			result[topic] = os.Stdout
		}
		return result, false, nil
	}

	err := os.MkdirAll(root, 0755)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to create the output directory")
	}

	for topic := range topics {
		file := filepath.Join(root, topic)
		lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
		}
		result[topic] = lf
	}

	return result, true, nil
}

func configureTLS() (*tls.Config, error) {
	var tlsConf tls.Config

	clientCert := tlsClientCert.Get()
	clientKey := tlsClientKey.Get()

	// Mutual authentication is enabled. Both client key and certificate are needed.
	if !internal.IsEmpty(clientCert) {
		if internal.IsEmpty(clientKey) {
			return nil, errors.New("TLS client key is missing. Mutual authentication cannot be used")
		}
		certificate, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to load the client TLS key pair")
		}
		tlsConf.Certificates = []tls.Certificate{certificate}
	}

	caCert := tlsCACert.Get()

	if internal.IsEmpty(caCert) {
		// Server cert verification will be disabled.
		// Only standard trusted certificates are used to verify the server certs.
		tlsConf.InsecureSkipVerify = true
		return &tlsConf, nil
	}
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(caCert)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read the CA certificate")
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append the CA certificate to the pool")
	}

	tlsConf.RootCAs = certPool

	return &tlsConf, nil
}

func getSearchColor(mode string) color.Style {
	switch mode {
	case internal.NoTheme:
		return nil
	case internal.DarkTheme:
		return color.New(color.FgYellow, color.Bold)
	case internal.LightTheme:
		return color.New(color.FgBlue, color.Bold)
	default:
		return nil
	}
}

func getColorTheme(mode string, toFile bool) internal.ColorTheme {
	theme := internal.ColorTheme{}
	if toFile {
		return theme
	}
	switch mode {
	case internal.DarkTheme:
		theme.Error = color.New(color.LightRed)
		theme.Info = color.New(color.LightGreen)
		theme.Warning = color.New(color.LightYellow)
	case internal.LightTheme:
		theme.Error = color.New(color.FgRed)
		theme.Info = color.New(color.FgGreen)
		theme.Warning = color.New(color.FgYellow)
	}
	return theme
}
