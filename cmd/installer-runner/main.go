package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/openshift/assisted-service/internal/installerserver"
	"github.com/openshift/assisted-service/pkg/generator"
	"github.com/openshift/assisted-service/pkg/s3wrapper"
	"github.com/sirupsen/logrus"
)

const (
	storageFilesystem = "filesystem"
	storageS3         = "s3"
)

type Config struct {
	GeneratorConfig generator.Config
	S3Config        s3wrapper.Config
	Storage         string `envconfig:"STORAGE" default:"s3"`
	WorkDir         string `envconfig:"WORK_DIR" default:"/data/"`
	LogLevel        string `envconfig:"LOG_LEVEL" default:"info"`
}

func main() {
	config := Config{}
	err := envconfig.Process("installer-runner", &config)
	if err != nil {
		fmt.Printf("Failed to parse envconfig: %s", err.Error())
		os.Exit(1)
	}

	log := createLogger(config.LogLevel)
	s3client := createStorageClient(config.Storage, config.S3Config, config.WorkDir, log)
	gen := generator.New(log, s3client, config.GeneratorConfig, config.WorkDir, nil)
	h := &installerserver.Handler{
		Log:       log,
		Generator: gen,
	}
	if err := serve(h, config.WorkDir, log); err != nil {
		log.WithError(err).Error("failed running http server")
		os.Exit(1)
	}
}

func createLogger(logLevel string) *logrus.Logger {
	log := logrus.New()
	log.SetReportCaller(true)

	log.Info("Setting Log Level: ", logLevel)
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		log.Error("Invalid Log Level: ", logLevel)
	} else {
		log.SetLevel(level)
	}

	return log
}

func createStorageClient(storage string, s3Config s3wrapper.Config, workdir string, log logrus.FieldLogger) s3wrapper.API {
	var storageClient s3wrapper.API = nil
	switch storage {
	case storageS3:
		if storageClient = s3wrapper.NewS3Client(&s3Config, log); storageClient == nil { //nolint:staticcheck
			log.Fatal("failed to create S3 client")
		}
	case storageFilesystem:
		storageClient = &s3wrapper.FSClient{
			Log:     log,
			BaseDir: workdir,
		}
	default:
		log.Fatalf("unsupported storage client: %s", storage)
	}
	return storageClient
}

func serve(h *installerserver.Handler, workdir string, log logrus.FieldLogger) error {
	server := http.Server{
		Handler:           h,
		ReadHeaderTimeout: 3 * time.Second,
	}
	if err := os.MkdirAll(filepath.Join(workdir, "installer-runner"), 0700); err != nil {
		return err
	}
	// TODO: Make this unique per base image version so two instances can run. Maybe through ENV?
	socketFile := filepath.Join(workdir, "installer-runner", "server.sock")

	unixListener, err := net.Listen("unix", socketFile)
	if err != nil {
		return err
	}

	// Interrupt server on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := server.Serve(unixListener); err != nil {
			log.WithError(err).Error("failed to run server")
		}
	}()

	<-stop
	if err := os.Remove(socketFile); err != nil {
		log.WithError(err).Errorf("failed to remove socket file %s", socketFile)
	}
	return server.Close()
}
