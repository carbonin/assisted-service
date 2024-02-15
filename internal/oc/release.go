package oc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/hashicorp/go-version"
	operatorv1alpha1 "github.com/openshift/api/operator/v1alpha1"
	"github.com/openshift/assisted-service/internal/common"
	"github.com/openshift/assisted-service/pkg/executer"
	"github.com/openshift/assisted-service/pkg/mirrorregistries"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/thedevsaddam/retry"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8syaml "sigs.k8s.io/yaml"
)

const (
	mcoImageName         = "machine-config-operator"
	ironicAgentImageName = "ironic-agent"
	mustGatherImageName  = "must-gather"
	okdRPMSImageName     = "okd-rpms"
	DefaultTries         = 5
	DefaltRetryDelay     = time.Second * 5
)

type Config struct {
	MaxTries   uint
	RetryDelay time.Duration
}

//go:generate mockgen -source=release.go -package=oc -destination=mock_release.go
type Release interface {
	GetMCOImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error)
	GetIronicAgentImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error)
	GetOKDRPMSImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error)
	GetMustGatherImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error)
	GetOpenshiftVersion(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error)
	GetMajorMinorVersion(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error)
	GetReleaseArchitecture(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) ([]string, error)
	GetReleaseBinaryPath(releaseImage string, cacheDir string) (workdir string, binary string, path string)
	Extract(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, cacheDir string, pullSecret string) (string, error)
}

type imageValue struct {
	value string
	mutex sync.Mutex
}

type release struct {
	executer                executer.Executer
	config                  Config
	mirrorRegistriesBuilder mirrorregistries.MirrorRegistriesConfigBuilder

	// A map for caching images (image name > release image URL > image)
	imagesMap common.ExpiringCache
}

func NewRelease(executer executer.Executer, config Config, mirrorRegistriesBuilder mirrorregistries.MirrorRegistriesConfigBuilder) Release {
	return &release{executer: executer, config: config, imagesMap: common.NewExpiringCache(cache.NoExpiration, cache.NoExpiration),
		mirrorRegistriesBuilder: mirrorRegistriesBuilder}
}

const ocAuthArgument = "--registry-config"

// GetMCOImage gets mcoImage url from the releaseImageMirror if provided.
// Else gets it from the source releaseImage
func (r *release) GetMCOImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error) {
	return r.getImageByName(log, mcoImageName, releaseImage, releaseImageMirror, pullSecret)
}

// GetIronicAgentImage gets the ironic agent image url from the releaseImageMirror if provided.
// Else gets it from the source releaseImage
func (r *release) GetIronicAgentImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error) {
	return r.getImageByName(log, ironicAgentImageName, releaseImage, releaseImageMirror, pullSecret)
}

// GetOKDRPMSImage gets okd RPMS image URL from the release image or releaseImageMirror, if provided.
func (r *release) GetOKDRPMSImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error) {
	return r.getImageByName(log, okdRPMSImageName, releaseImage, releaseImageMirror, pullSecret)
}

// GetMustGatherImage gets must-gather image URL from the release image or releaseImageMirror, if provided.
func (r *release) GetMustGatherImage(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error) {
	return r.getImageByName(log, mustGatherImageName, releaseImage, releaseImageMirror, pullSecret)
}

func (r *release) getImageByName(log logrus.FieldLogger, imageName, releaseImage, releaseImageMirror, pullSecret string) (string, error) {
	var image string
	var err error
	if releaseImage == "" && releaseImageMirror == "" {
		return "", errors.New("neither releaseImage, nor releaseImageMirror are provided")
	}

	icspFile, err := r.getIcspFileFromRegistriesConfig(log)
	if err != nil {
		return "", errors.Wrap(err, "failed to create file ICSP file from registries config")
	}
	defer removeIcspFile(icspFile)

	if releaseImageMirror != "" {
		//TODO: Get mirror registry certificate from install-config
		image, err = r.getImageFromRelease(log, imageName, releaseImageMirror, pullSecret, icspFile, true)
		if err != nil {
			log.WithError(err).Errorf("failed to get %s image from mirror release image %s", imageName, releaseImageMirror)
			return "", err
		}
	} else {
		image, err = r.getImageFromRelease(log, imageName, releaseImage, pullSecret, icspFile, false)
		if err != nil {
			log.WithError(err).Errorf("failed to get %s image from release image %s", imageName, releaseImage)
			return "", err
		}
	}
	return image, err
}

func (r *release) GetOpenshiftVersion(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error) {
	var openshiftVersion string
	var err error
	if releaseImage == "" && releaseImageMirror == "" {
		return "", errors.New("no releaseImage nor releaseImageMirror provided")
	}

	icspFile, err := r.getIcspFileFromRegistriesConfig(log)
	if err != nil {
		return "", errors.Wrap(err, "failed to create file ICSP file from registries config")
	}
	defer removeIcspFile(icspFile)

	if releaseImageMirror != "" {
		//TODO: Get mirror registry certificate from install-config
		openshiftVersion, err = r.getOpenshiftVersionFromRelease(log, releaseImageMirror, pullSecret, icspFile, true)
		if err != nil {
			log.WithError(err).Errorf("failed to get image openshift version from mirror release image %s", releaseImageMirror)
			return "", err
		}
	} else {
		openshiftVersion, err = r.getOpenshiftVersionFromRelease(log, releaseImage, pullSecret, icspFile, false)
		if err != nil {
			log.WithError(err).Errorf("failed to get image openshift version from release image %s", releaseImage)
			return "", err
		}
	}

	return openshiftVersion, err
}

func (r *release) GetMajorMinorVersion(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) (string, error) {
	openshiftVersion, err := r.GetOpenshiftVersion(log, releaseImage, releaseImageMirror, pullSecret)
	if err != nil {
		return "", err
	}

	v, err := version.NewVersion(openshiftVersion)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d.%d", v.Segments()[0], v.Segments()[1]), nil
}

func (r *release) GetReleaseArchitecture(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, pullSecret string) ([]string, error) {
	image := releaseImageMirror
	if image == "" {
		image = releaseImage
	}
	if image == "" {
		return nil, errors.New("no releaseImage nor releaseImageMirror provided")
	}

	icspFile, err := r.getIcspFileFromRegistriesConfig(log)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file ICSP file from registries config")
	}
	defer removeIcspFile(icspFile)

	args := []string{"image", "info", "--output", "json"}
	if icspFile != "" {
		args = append(args, fmt.Sprintf("--icsp-file=%s", icspFile))
	}
	args = append(args, image)

	imageInfoStr, ocError := executeWithPullSecretFile(log, r.executer, pullSecret, ocAuthArgument, "oc", args...)
	if ocError != nil {
		// TODO(WRKLDS-222) At this moment we don't have a better way to detect if the release image is a multiarch
		//                  image. Introducing skopeo as an additional dependency, to be able to manually parse
		//                  the manifest. https://bugzilla.redhat.com/show_bug.cgi?id=2111537 tracks the missing
		//                  feature in oc cli.
		skopeoImageRaw, skopeoError := executeWithPullSecretFile(log, r.executer, pullSecret, "--authfile", "skopeo", "inspect", "--raw", "--no-tags", fmt.Sprintf("docker://%s", image))
		if skopeoError != nil {
			return nil, errors.Errorf("failed to inspect image, oc: %v, skopeo: %v", err, skopeoError)
		}

		var multiarchContent []string
		_, err2 := jsonparser.ArrayEach([]byte(skopeoImageRaw), func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			res, _ := jsonparser.GetString(value, "platform", "architecture")

			// Convert architecture naming to supported values
			res = common.NormalizeCPUArchitecture(res)
			if res == "" {
				return
			}

			multiarchContent = append(multiarchContent, res)
		}, "manifests")
		if err2 != nil {
			return nil, errors.Errorf("failed to get image info using oc: %v", err)
		}

		if len(multiarchContent) == 0 {
			return nil, errors.Errorf("image manifest does not contain architecture: %v", skopeoImageRaw)
		}

		return multiarchContent, nil
	}

	architecture, err := jsonparser.GetString([]byte(imageInfoStr), "config", "architecture")
	if err != nil {
		return nil, err
	}

	// Convert architecture naming to supported values
	architecture = common.NormalizeCPUArchitecture(architecture)

	return []string{architecture}, nil
}

func getImageKey(imageName, releaseImage string) string {
	return imageName + "@" + releaseImage
}

func (r *release) getImageValue(imageName, releaseImage string) (*imageValue, error) {
	actualIntf, _ := r.imagesMap.GetOrInsert(getImageKey(imageName, releaseImage), &imageValue{})
	value, ok := actualIntf.(*imageValue)
	if !ok {
		return nil, errors.Errorf("unexpected error - could not cast value for image %s release %s", imageName, releaseImage)
	}
	return value, nil
}

func (r *release) getImageFromRelease(log logrus.FieldLogger, imageName, releaseImage, pullSecret, icspFile string, insecure bool) (string, error) {
	// Fetch image URL from cache
	actualImageValue, err := r.getImageValue(imageName, releaseImage)
	if err != nil {
		return "", err
	}
	if actualImageValue.value != "" {
		return actualImageValue.value, nil
	}
	actualImageValue.mutex.Lock()
	defer actualImageValue.mutex.Unlock()
	if actualImageValue.value != "" {
		return actualImageValue.value, nil
	}

	args := []string{"adm", "release", "info", "--image-for=" + imageName, "--insecure=" + strconv.FormatBool(insecure)}
	if icspFile != "" {
		args = append(args, "--icsp-file="+icspFile)
	}
	args = append(args, releaseImage)

	log.Infof("Fetching image from OCP release (oc %s)", strings.Join(args, " "))
	image, err := executeWithPullSecretFile(log, r.executer, pullSecret, ocAuthArgument, "oc", args...)
	if err != nil {
		return "", err
	}

	// Update image URL in cache
	actualImageValue.value = image

	return image, nil
}

func (r *release) getOpenshiftVersionFromRelease(log logrus.FieldLogger, releaseImage, pullSecret, icspFile string, insecure bool) (string, error) {
	args := []string{"adm", "release", "info", "-o", "template", "--template", "'{{.metadata.version}}'", "--insecure=" + strconv.FormatBool(insecure)}
	if icspFile != "" {
		args = append(args, "--icsp-file", icspFile)
	}
	args = append(args, releaseImage)

	version, err := executeWithPullSecretFile(log, r.executer, pullSecret, ocAuthArgument, "oc", args...)
	if err != nil {
		return "", err
	}
	// Trimming as output is retrieved wrapped with single quotes.
	return strings.Trim(version, "'"), nil
}

// Extract openshift-baremetal-install binary from releaseImageMirror if provided.
// Else extract from the source releaseImage
func (r *release) Extract(log logrus.FieldLogger, releaseImage string, releaseImageMirror string, cacheDir string, pullSecret string) (string, error) {
	var path string
	var err error
	if releaseImage == "" && releaseImageMirror == "" {
		return "", errors.New("no releaseImage or releaseImageMirror provided")
	}

	icspFile, err := r.getIcspFileFromRegistriesConfig(log)
	if err != nil {
		return "", errors.Wrap(err, "failed to create file ICSP file from registries config")
	}
	defer removeIcspFile(icspFile)

	if releaseImageMirror != "" {
		//TODO: Get mirror registry certificate from install-config
		path, err = r.extractFromRelease(log, releaseImageMirror, cacheDir, pullSecret, true, icspFile)
		if err != nil {
			log.WithError(err).Errorf("failed to extract openshift-baremetal-install from mirror release image %s", releaseImageMirror)
			return "", err
		}
	} else {
		path, err = r.extractFromRelease(log, releaseImage, cacheDir, pullSecret, false, icspFile)
		if err != nil {
			log.WithError(err).Errorf("failed to extract openshift-baremetal-install from release image %s", releaseImage)
			return "", err
		}
	}
	return path, err
}

func (r *release) GetReleaseBinaryPath(releaseImage string, cacheDir string) (workdir string, binary string, path string) {
	workdir = filepath.Join(cacheDir, releaseImage)
	binary = "openshift-baremetal-install"
	path = filepath.Join(workdir, binary)
	return
}

// extractFromRelease returns the path to an openshift-baremetal-install binary extracted from
// the referenced release image.
func (r *release) extractFromRelease(log logrus.FieldLogger, releaseImage, cacheDir, pullSecret string, insecure bool, icspFile string) (string, error) {
	workdir, binary, path := r.GetReleaseBinaryPath(releaseImage, cacheDir)
	log.Infof("extracting %s binary to %s", binary, workdir)
	err := os.MkdirAll(workdir, 0755)
	if err != nil {
		return "", err
	}

	args := []string{"adm", "release", "extract", "--command=" + binary, "--to=" + workdir, "--insecure=" + strconv.FormatBool(insecure)}
	if icspFile != "" {
		args = append(args, "--icsp-file="+icspFile)
	}
	args = append(args, releaseImage)

	retryFunc := func() error {
		_, err := executeWithPullSecretFile(log, r.executer, pullSecret, ocAuthArgument, "oc", args...)
		return err
	}

	_, err = retry.Do(r.config.MaxTries, r.config.RetryDelay, retryFunc)
	if err != nil {
		return "", err
	}

	log.Infof("Successfully extracted %s binary from the release to: %s", binary, path)
	return path, nil
}

func executeWithPullSecretFile(log logrus.FieldLogger, executer executer.Executer, pullSecret string, pullSecretFileArg string, command string, args ...string) (string, error) {
	// write pull secret to a temp file
	ps, err := executer.TempFile("", "registry-config")
	if err != nil {
		return "", err
	}
	defer func() {
		ps.Close()
		os.Remove(ps.Name())
	}()
	_, err = ps.Write([]byte(pullSecret))
	if err != nil {
		return "", err
	}
	// flush the buffer to ensure the file can be read
	ps.Close()

	args = append(args, fmt.Sprintf("%s=%s", pullSecretFileArg, ps.Name()))
	stdout, stderr, exitCode := executer.Execute(command, args...)
	if exitCode == 0 {
		return strings.TrimSpace(stdout), nil
	} else {
		err := fmt.Errorf("command '%s' exited with non-zero exit code %d: %s\n%s", append([]string{command}, args...), exitCode, stdout, stderr)
		log.Warn(err)
		return "", err
	}
}

// combinedCAFile creates a temp directory containing a combined CA file which includes the host default CA bundle
// as well as the mirror registry certs provided by the user. The directory of the file returned may be used
// if the command requires a directory rather than a cert file. It is an error to call this function if mirror registry
// certs are not defined.
func (r *release) combinedCAFile(log logrus.FieldLogger) (string, error) {
	dir, err := os.MkdirTemp("", "oc-release-combined-ca-file")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	userCABundle, err := r.mirrorRegistriesBuilder.GetMirrorCA()
	if err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("failed to read mirror CA bundle: %w", err)
	}

	defaultCABundle, err := os.ReadFile("/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem")
	if err != nil {
		log.Warn("failed to red default TLS CA bundle, using only user-provided certs")
		defaultCABundle = []byte{}
	}

	finalCABundle := append(userCABundle, '\n', defaultCABundle...)
	//os.WriteFile(
}

// Create a temporary file containing the ImageContentPolicySources
func (r *release) getIcspFileFromRegistriesConfig(log logrus.FieldLogger) (string, error) {

	if !r.mirrorRegistriesBuilder.IsMirrorRegistriesConfigured() {
		log.Debugf("No mirrors configured to build ICSP file")
		return "", nil
	}

	mirrorRegistriesConfig, err := r.mirrorRegistriesBuilder.ExtractLocationMirrorDataFromRegistries()
	if err != nil {
		log.WithError(err).Errorf("Failed to get the mirror registries needed for ImageContentSources")
		return "", err
	}

	contents, err := getIcspContents(mirrorRegistriesConfig)
	if err != nil {
		log.WithError(err).Errorf("Failed to create the ICSP file from registries.conf")
		return "", err
	}
	if contents == nil {
		log.Debugf("No registry entries to build ICSP file")
		return "", nil
	}

	icspFile, err := os.CreateTemp("", "icsp-file")
	if err != nil {
		return "", err
	}
	log.Debugf("Building ICSP file from registries.conf with contents %s", contents)
	if _, err := icspFile.Write(contents); err != nil {
		icspFile.Close()
		os.Remove(icspFile.Name())
		return "", err
	}
	icspFile.Close()

	return icspFile.Name(), nil
}

// Convert the data in registries.conf into ICSP format
func getIcspContents(mirrorConfig []mirrorregistries.RegistriesConf) ([]byte, error) {

	icsp := operatorv1alpha1.ImageContentSourcePolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: operatorv1alpha1.SchemeGroupVersion.String(),
			Kind:       "ImageContentSourcePolicy",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "image-policy",
			// not namespaced
		},
	}

	icsp.Spec.RepositoryDigestMirrors = make([]operatorv1alpha1.RepositoryDigestMirrors, len(mirrorConfig))
	for i, mirrorRegistries := range mirrorConfig {
		icsp.Spec.RepositoryDigestMirrors[i] = operatorv1alpha1.RepositoryDigestMirrors{Source: mirrorRegistries.Location, Mirrors: mirrorRegistries.Mirror}
	}

	// Convert to json first so json tags are handled
	jsonData, err := json.Marshal(&icsp)
	if err != nil {
		return nil, err
	}
	contents, err := k8syaml.JSONToYAML(jsonData)
	if err != nil {
		return nil, err
	}

	return contents, nil
}

func removeIcspFile(filename string) {
	if filename != "" {
		os.Remove(filename)
	}
}
