package installerserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/openshift/assisted-service/internal/common"
	"github.com/openshift/assisted-service/models"
	"github.com/openshift/assisted-service/pkg/generator"
	"github.com/sirupsen/logrus"
)

func TestInstallerServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "installerserver tests")
}

var _ = Describe("Handler", func() {
	var (
		ctrl          *gomock.Controller
		mockGenerator *generator.MockInstallConfigGenerator

		server *httptest.Server
		client *http.Client

		clusterID     = strfmt.UUID("01e13368-44e0-4c57-a0e7-23693c74b191")
		installConfig = []byte("installconfig")
		releaseImage  = "example.com/repo/image:tag"
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		mockGenerator = generator.NewMockInstallConfigGenerator(ctrl)
		handler := Handler{Log: logrus.New(), Generator: mockGenerator}
		server = httptest.NewServer(&handler)
		client = server.Client()
	})

	AfterEach(func() {
		server.Close()
		ctrl.Finish()
	})

	validInputDataReader := func() io.Reader {
		data := generator.InputData{
			Cluster:       common.Cluster{Cluster: models.Cluster{ID: &clusterID}},
			InstallConfig: installConfig,
			ReleaseImage:  releaseImage,
		}
		jsonData, err := json.Marshal(data)
		Expect(err).To(BeNil())
		return bytes.NewReader(jsonData)
	}

	It("succeeds when GenerateInstallConfig succeeds", func() {
		mockGenerator.EXPECT().GenerateInstallConfig(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, genData generator.InputData) error {
				Expect(genData.Cluster.ID).To(HaveValue(Equal(clusterID)))
				Expect(genData.InstallConfig).To(Equal(installConfig))
				Expect(genData.ReleaseImage).To(Equal(releaseImage))
				return nil
			},
		)

		resp, err := client.Post(server.URL+"/run-install", "application/json", validInputDataReader())
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})

	It("fails for paths other than run-install", func() {
		resp, err := client.Post(server.URL+"/things", "application/json", validInputDataReader())
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
	})

	It("fails for methods other than POST", func() {
		resp, err := client.Get(server.URL + "/run-install")
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("fails if more than 100M is provided in the body", func() {
	})

	It("fails if the body isn't properly formatted json", func() {
	})

	It("fails if the json provided is not a correct generator.InputData", func() {
	})

	It("returns the error from GenerateInstallConfig if it fails", func() {
	})
})
