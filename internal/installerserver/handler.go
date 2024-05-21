package installerserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/openshift/assisted-service/pkg/generator"
	"github.com/sirupsen/logrus"
)

type Handler struct {
	Log       logrus.FieldLogger
	Generator generator.InstallConfigGenerator
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Log.Infof("Handling %s request to path %s", r.Method, r.URL.Path)

	if r.URL.Path != "/run-install" {
		h.httpErrorf(w, http.StatusNotFound, "Path %s, not found", r.URL.Path)
		return
	}
	if r.Method != http.MethodPost {
		h.httpErrorf(w, http.StatusBadRequest, "Method %s not supported", r.Method)
		return
	}

	bodyReader := &io.LimitedReader{R: r.Body, N: 100_000_000}
	bodyData, err := io.ReadAll(bodyReader)
	if err != nil {
		h.httpErrorf(w, http.StatusInternalServerError, "Failed to read request body")
		return
	}
	if bodyReader.N <= 0 {
		h.httpErrorf(w, http.StatusBadRequest, "Request body exceeds the limit of 100MB")
		return
	}

	genData := generator.InputData{}
	if err := json.Unmarshal(bodyData, &genData); err != nil {
		h.httpErrorf(w, http.StatusBadRequest, "Failed to unmarshal input data: %s", err.Error())
		return
	}

	h.Log.Infof("got request to generate install manifests for cluster %s, with install config '%s', and release image '%s'", genData.Cluster.ID, genData.InstallConfig, genData.ReleaseImage)
	if err := h.Generator.GenerateInstallConfig(r.Context(), genData); err != nil {
		h.httpErrorf(w, http.StatusInternalServerError, err.Error())
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) httpErrorf(w http.ResponseWriter, code int, format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	h.Log.Error(msg)
	http.Error(w, msg, code)
}
