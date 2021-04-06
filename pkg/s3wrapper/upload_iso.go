package s3wrapper

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/openshift/assisted-service/internal/isoeditor"
	logutil "github.com/openshift/assisted-service/pkg/log"
	"github.com/openshift/assisted-service/pkg/staticnetworkconfig"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const minimumPartSizeBytes = 5 * 1024 * 1024    // 5MB
const copyPartChunkSizeBytes = 64 * 1024 * 1024 // 64MB

type ISOUploaderAPI interface {
	UploadISO(ctx context.Context, ignitionConfig string, staticNetworkConfig string, proxyInfo *isoeditor.ClusterProxyInfo, srcObjectName string, destObjectName string) error
}

var _ ISOUploaderAPI = &ISOUploader{}

type ISOUploader struct {
	log              logrus.FieldLogger
	s3client         s3iface.S3API
	bucket           string
	publicBucket     string
	infoCache        []isoInfo
	netConfGenerator staticnetworkconfig.StaticNetworkConfig
}

type isoInfo struct {
	etag           string
	baseObjectSize int64
	ignOffsetBytes int64
	ignLengthBytes int64
	rdOffsetBytes  int64
	rdLengthBytes  int64
	minimal        bool
}

func NewISOUploader(logger logrus.FieldLogger, s3Client s3iface.S3API, bucket, publicBucket string, staticNetworkConfig staticnetworkconfig.StaticNetworkConfig) *ISOUploader {
	return &ISOUploader{log: logger, s3client: s3Client, bucket: bucket, publicBucket: publicBucket, netConfGenerator: staticNetworkConfig}
}

func (u *ISOUploader) UploadISO(ctx context.Context, ignitionConfig string, staticNetworkConfig string, proxyInfo *isoeditor.ClusterProxyInfo, srcObjectName string, destObjectName string) error {
	log := logutil.FromContext(ctx, u.log)
	log.Debugf("Started upload of ISO %s", destObjectName)

	baseISOInfo, origIgnContents, origRdContents, err := u.getISOInfo(srcObjectName, log)
	if err != nil {
		err = errors.Wrapf(err, "Failed to fetch base ISO information")
		log.Error(err)
		return err
	}

	// If the two embedded areas are within the same part we're gonna have a bad time
	// TODO make this work - I don't think we can guarentee 5MB between these files
	if baseISOInfo.minimal && math.Abs(float64(baseISOInfo.ignOffsetBytes-baseISOInfo.rdOffsetBytes)) < minimumPartSizeBytes {
		err = errors.New("Ignition and ramdisk reserved areas are within the minimum part size, this is not supported")
		log.Error(err)
		return err
	}

	upload := multiUpload{
		ctx:             ctx,
		log:             log,
		uploader:        u,
		isoInfo:         baseISOInfo,
		origIgnContents: origIgnContents,
		origRdContents:  origRdContents,
		sourceObjectKey: srcObjectName,
		destObjectKey:   destObjectName,
	}
	err = upload.Upload(ignitionConfig, staticNetworkConfig, proxyInfo)
	if err != nil {
		err = errors.Wrapf(err, "Failed to create ISO %s", destObjectName)
		log.Error(err)
		return err
	}
	return nil
}

func (u *ISOUploader) getISOInfo(baseObjectName string, log logrus.FieldLogger) (*isoInfo, *[]byte, *[]byte, error) {
	// Get ETag from S3
	headResp, err := u.s3client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(u.publicBucket),
		Key:    aws.String(baseObjectName),
	})
	if err != nil {
		log.WithError(err).Errorf("Failed to fetch metadata for base object %s", baseObjectName)
		return nil, nil, nil, err
	}

	// See if the ISO info is cached
	var found *isoInfo
	for i := range u.infoCache {
		if u.infoCache[i].etag == *headResp.ETag {
			found = &u.infoCache[i]
		}
	}
	cachePath := filepath.Join("/tmp", *headResp.ETag)
	ignCachePath := filepath.Join(cachePath, "ignition")
	rdCachePath := filepath.Join(cachePath, "ramdisk")

	var info isoInfo
	var ignContents, rdContents *[]byte

	if found == nil {
		// Add to cache if not found
		log.Infof("Did not find ISO info for %s in cache, will add", baseObjectName)
		info.etag = *headResp.ETag
		info.baseObjectSize = *headResp.ContentLength

		var offset, length int64
		rdInfo, ignInfo, err := u.getISOHeaderInfo(log, baseObjectName, *headResp.ContentLength)
		if err != nil {
			err = errors.Wrapf(err, "Failed to get base ISO info for %s from S3", baseObjectName)
			log.Error(err)
			return nil, nil, nil, err
		}

		info.ignLengthBytes = int64(ignInfo.Length)
		info.ignOffsetBytes = int64(ignInfo.Offset)
		ignContents, err = u.cacheOriginalPart(baseObjectName, info.ignOffsetBytes)
		if err != nil {
			log.Error(err)
			return nil, nil, nil, err
		}

		if rdInfo != nil {
			info.minimal = true
			info.rdLengthBytes = int64(rdInfo.Length)
			info.rdOffsetBytes = int64(rdInfo.Offset)
			rdContents, err = u.cacheOriginalPart(baseObjectName, info.rdOffsetBytes)
			if err != nil {
				log.Error(err)
				return nil, nil, nil, err
			}
		}
		u.addToInfoCache(ignContents, ignCachePath, rdContents, rdCachePath, &info, log)
	} else {
		// Return from cache
		log.Debugf("Found ISO info for %s in cache", baseObjectName)
		info = *found

		ignBytes, err := ioutil.ReadFile(ignCachePath)
		if err != nil {
			err = errors.Wrapf(err, "Failed to fetch ignition embedded area of live ISO %s from disk cache", baseObjectName)
			log.Error(err)
			return nil, nil, nil, err
		}
		ignContents = &ignBytes

		if info.minimal {
			rdBytes, err := ioutil.ReadFile(rdCachePath)
			if err != nil {
				err = errors.Wrapf(err, "Failed to fetch ramdisk embedded area of minimal live ISO %s from disk cache", baseObjectName)
				log.Error(err)
				return nil, nil, nil, err
			}
			rdContents = &rdBytes
		}
	}
	return &info, ignContents, rdContents, nil
}

func (u *ISOUploader) addToInfoCache(ignContents *[]byte, ignPath string, rdContents *[]byte, rdPath string, info *isoInfo, log logrus.FieldLogger) {
	// If we fail anywhere in this function, continue without adding to cache rather than failing the entire operation
	err := ioutil.WriteFile(ignPath, *ignContents, 0600)
	if err != nil {
		log.WithError(err).Errorf("failed to cache ignition embedded area to file %s", ignPath)
		return
	}

	err = ioutil.WriteFile(rdPath, *rdContents, 0600)
	if err != nil {
		log.WithError(err).Errorf("failed to cache ramdisk embedded area to file %s", rdPath)
		os.Remove(ignPath)
		return
	}
	u.infoCache = append(u.infoCache, *info)
}

func (u *ISOUploader) cacheOriginalPart(baseObjectName string, offset int64) (*[]byte, error) {
	var getRest *s3.GetObjectOutput
	var err error
	getRest, err = u.s3client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(u.publicBucket),
		Key:    aws.String(baseObjectName),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+minimumPartSizeBytes-1)),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to fetch embedded area of live ISO %s", baseObjectName)
	}

	origContents, err := ioutil.ReadAll(getRest.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read body from embedded area of live ISO %s", baseObjectName)
	}

	return &origContents, nil
}

func (u *ISOUploader) getISOHeaderInfo(log logrus.FieldLogger, baseObjectName string, baseObjectSize int64) (*isoeditor.OffsetInfo, *isoeditor.OffsetInfo, error) {
	// Download header of the live ISO (last 48 bytes of the first 32KB)
	getResp, err := u.s3client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(u.publicBucket),
		Key:    aws.String(baseObjectName),
		Range:  aws.String("bytes=32720-32767"),
	})
	if err != nil {
		log.WithError(err).Errorf("Failed to get header of object %s from bucket %s", baseObjectName, u.publicBucket)
		return nil, nil, err
	}
	headerString, err := ioutil.ReadAll(getResp.Body)
	if err != nil {
		log.WithError(err).Errorf("Failed to read header of object %s from bucket %s", baseObjectName, u.publicBucket)
		return nil, nil, err
	}

	ramdiskHeader := headerString[0:24]
	rdOffsetInfo, err := isoeditor.GetRamDiskArea(ramdiskHeader)
	if err != nil {
		log.WithError(err).Warnf("Failed to read ramdisk embed info from %s, assuming full iso", baseObjectName)
		rdOffsetInfo = nil
	} else {
		if err := validateEmbedArea(rdOffsetInfo, baseObjectSize); err != nil {
			return nil, nil, err
		}
	}

	ignitionHeader := headerString[24:48]
	ignOffsetInfo, err := isoeditor.GetIgnitionArea(ignitionHeader)
	if err != nil {
		return nil, nil, err
	}

	if err := validateEmbedArea(ignOffsetInfo, baseObjectSize); err != nil {
		return nil, nil, err
	}

	return rdOffsetInfo, ignOffsetInfo, nil
}

func validateEmbedArea(info *isoeditor.OffsetInfo, baseObjectSize int64) error {
	// For now we assume that the embedded area is less than 5MB, which is the minimum S3 part size
	if info.Length > minimumPartSizeBytes {
		return errors.New("ISO embedded area is larger than what is currently supported")
	}

	if info.Offset+minimumPartSizeBytes > uint64(baseObjectSize) {
		return errors.New("Embedded area is too close to the end of the file, which is currently not handled")
	}

	return nil
}

type multiUpload struct {
	ctx             context.Context
	log             logrus.FieldLogger
	uploader        *ISOUploader
	isoInfo         *isoInfo
	origIgnContents *[]byte
	origRdContents  *[]byte
	wg              sync.WaitGroup
	mutex           sync.Mutex
	err             error
	uploadID        string
	sourceObjectKey string
	destObjectKey   string
	parts           completedParts
}

type chunk struct {
	partNum          int64
	sourceStartBytes int64
	sourceEndBytes   int64
}

// completedParts is a wrapper to make parts sortable by their part number,
// since S3 required this list to be sent in sorted order.
type completedParts []*s3.CompletedPart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

func (m *multiUpload) Upload(ignitionConfig string, staticNetworkConfig string, proxyInfo *isoeditor.ClusterProxyInfo) error {
	m.log.Debugf("Creating multi-part upload of ISO %s", m.destObjectKey)
	multiOut, err := m.uploader.s3client.CreateMultipartUploadWithContext(
		m.ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(m.uploader.bucket), Key: aws.String(m.destObjectKey)})
	if err != nil {
		err = errors.Wrapf(err, "Failed to start upload for %s", m.destObjectKey)
		m.log.Error(err)
		return err
	}
	m.uploadID = *multiOut.UploadId

	m.log.Debugf("Starting goroutines copying chunks for ISO %s", m.destObjectKey)
	maxChunks := int(m.isoInfo.baseObjectSize/copyPartChunkSizeBytes) + 1
	ch := make(chan chunk, maxChunks)
	for i := 0; i < maxChunks; i++ {
		m.wg.Add(1)
		go m.copyChunk(ch)
	}

	ignAreaPartNum, rdAreaPartNum := m.generateWorkForISO(ch)
	close(ch)

	m.log.Debugf("Uploading embedded area (compressed ignition config) for ISO %s", m.destObjectKey)
	err = m.uploadCustomParts(m.log, ignAreaPartNum, rdAreaPartNum, ignitionConfig, staticNetworkConfig, proxyInfo)
	if err != nil {
		m.log.Error(err)
		m.seterr(err)
	} else {
		m.log.Debugf("Completed upload of embedded area for ISO %s, waiting for async copies", m.destObjectKey)
	}

	// We now finished sending the chunks to copy as well as uploading the embedded area, wait for copies to finish
	m.wg.Wait()
	m.complete()

	if err := m.geterr(); err != nil {
		return err
	}
	m.log.Debugf("Completed upload of ISO %s", m.destObjectKey)
	return nil
}

func (m *multiUpload) generateWorkForISO(ch chan chunk) (ignAreaPartNum int64, rdAreaPartNum int64) {
	m.log.Debugf("Providing work for goroutines copying chunks for ISO %s", m.destObjectKey)

	// only need ramdisk for minimal iso
	// TODO refactor this somehow?
	if m.isoInfo.minimal {
		if m.isoInfo.ignOffsetBytes < m.isoInfo.rdOffsetBytes {
			ignAreaPartNum = m.generateWorkForRange(ch, int64(1), 0, m.isoInfo.ignOffsetBytes-1)
			rdAreaPartNum = m.generateWorkForRange(ch, ignAreaPartNum+1, m.isoInfo.ignOffsetBytes+minimumPartSizeBytes, m.isoInfo.rdOffsetBytes-1)
			m.generateWorkForRange(ch, rdAreaPartNum+1, m.isoInfo.rdOffsetBytes+minimumPartSizeBytes, m.isoInfo.baseObjectSize-1)
		} else if m.isoInfo.rdOffsetBytes < m.isoInfo.ignOffsetBytes {
			rdAreaPartNum = m.generateWorkForRange(ch, int64(1), 0, m.isoInfo.rdOffsetBytes-1)
			ignAreaPartNum = m.generateWorkForRange(ch, rdAreaPartNum+1, m.isoInfo.rdOffsetBytes+minimumPartSizeBytes, m.isoInfo.ignOffsetBytes-1)
			m.generateWorkForRange(ch, ignAreaPartNum+1, m.isoInfo.ignOffsetBytes+minimumPartSizeBytes, m.isoInfo.baseObjectSize-1)
		} else {
			// WAT, create an error for this?
			panic("ignition offset and ramdisk offset should never be the same")
		}
	} else {
		ignAreaPartNum = m.generateWorkForRange(ch, int64(1), 0, m.isoInfo.ignOffsetBytes-1)
		m.generateWorkForRange(ch, ignAreaPartNum+1, m.isoInfo.ignOffsetBytes+minimumPartSizeBytes, m.isoInfo.baseObjectSize-1)
	}

	return
}

func (m *multiUpload) copyChunk(ch chan chunk) {
	defer m.wg.Done()
	for {
		data, ok := <-ch

		if !ok {
			break
		}

		if m.geterr() == nil {
			if err := m.uploadPartCopy(data); err != nil {
				m.seterr(err)
			}
		}
	}
}

func (m *multiUpload) generateWorkForRange(ch chan chunk, partNum int64, startOffsetBytes int64, endOffsetBytes int64) int64 {
	var (
		currentChunkEnd int64
		nextChunkStart  int64
		offsetCounter   = startOffsetBytes
		partCounter     = partNum
	)
	for m.geterr() == nil && offsetCounter < endOffsetBytes {
		currentChunkEnd = offsetCounter + copyPartChunkSizeBytes - 1
		nextChunkStart = currentChunkEnd + 1
		// We need to check two conditions which will cause this to be the last part:
		// 1. If we're at the end of the specified range, we need to copy less than the chunk size
		// 2. If the next chunk will be less than 5MB, we need to fold it into this one
		if (currentChunkEnd > endOffsetBytes-1) || (nextChunkStart+minimumPartSizeBytes-1 > endOffsetBytes) {
			currentChunkEnd = endOffsetBytes
			nextChunkStart = endOffsetBytes
		}

		ch <- chunk{partNum: partCounter, sourceStartBytes: offsetCounter, sourceEndBytes: currentChunkEnd}
		offsetCounter = nextChunkStart
		partCounter++
	}
	return partCounter
}

func (m *multiUpload) geterr() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.err
}

func (m *multiUpload) seterr(e error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.err = e
}

func (m *multiUpload) appendCompletedPart(completed *s3.CompletedPart) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.parts = append(m.parts, completed)
}

func (m *multiUpload) complete() {
	err := m.geterr()
	if err != nil {
		m.fail()
		return
	}

	// Parts must be sorted in PartNumber order.
	sort.Sort(m.parts)

	params := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(m.uploader.bucket),
		Key:             aws.String(m.destObjectKey),
		UploadId:        aws.String(m.uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: m.parts},
	}
	_, err = m.uploader.s3client.CompleteMultipartUploadWithContext(m.ctx, params)
	if err != nil {
		err = errors.Wrapf(err, "Failed to complete upload for %s", m.destObjectKey)
		m.log.Error(err)
		m.seterr(err)
		m.fail()
	}
}

func (m *multiUpload) fail() {
	// using new context because m.ctx may be canceled and it will fail the operation
	_, err := m.uploader.s3client.AbortMultipartUploadWithContext(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(m.uploader.bucket),
		Key:      aws.String(m.destObjectKey),
		UploadId: aws.String(m.uploadID),
	})
	if err != nil {
		m.log.WithError(err).Warnf("Failed to abort failed multipart upload with ID %s", m.uploadID)
	}
}

func (m *multiUpload) uploadPartCopy(c chunk) error {
	completedPartCopy, err := m.uploader.s3client.UploadPartCopyWithContext(m.ctx, &s3.UploadPartCopyInput{
		Bucket:          aws.String(m.uploader.bucket),
		Key:             aws.String(m.destObjectKey),
		CopySource:      aws.String(fmt.Sprintf("/%s/%s", m.uploader.publicBucket, m.sourceObjectKey)),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", c.sourceStartBytes, c.sourceEndBytes)),
		PartNumber:      aws.Int64(c.partNum),
		UploadId:        aws.String(m.uploadID),
	})
	if err != nil {
		err = errors.Wrapf(err, "Failed to copy part %d for file %s", c.partNum, m.destObjectKey)
		m.log.Error(err)
		return err
	}
	m.appendCompletedPart(&s3.CompletedPart{ETag: completedPartCopy.CopyPartResult.ETag, PartNumber: aws.Int64(c.partNum)})
	return nil
}

func (m *multiUpload) uploadCustomParts(log logrus.FieldLogger, ignPartNum int64, rdPartNum int64, ignitionConfig string, staticNetworkConfig string, proxyInfo *isoeditor.ClusterProxyInfo) error {
	ignitionBytes, err := isoeditor.IgnitionImageArchive(ignitionConfig)
	if err != nil {
		return errors.Wrap(err, "failed to generate ignition image archive")
	}

	if int64(len(ignitionBytes)) > m.isoInfo.ignLengthBytes {
		return errors.New(fmt.Sprintf("ignition is too long to be embedded (%d > %d)", len(ignitionBytes), m.isoInfo.ignLengthBytes))
	}

	if err := m.uploadCustomPart(m.origIgnContents, ignitionBytes, ignPartNum); err != nil {
		return errors.Wrapf(err, "failed to upload ignition for file %s", m.destObjectKey)
	}

	if m.isoInfo.minimal && (staticNetworkConfig != "" || !proxyInfo.Empty()) {
		var filesList []staticnetworkconfig.StaticNetworkConfigData
		var err error
		if staticNetworkConfig != "" {
			filesList, err = m.uploader.netConfGenerator.GenerateStaticNetworkConfigData(staticNetworkConfig)
			if err != nil {
				return errors.Wrap(err, "failed to generate static network config files")
			}
		}

		ramdiskBytes, err := isoeditor.RamDiskImageArchive(filesList, proxyInfo)
		if err != nil {
			return errors.Wrap(err, "failed to create ramdisk image")
		}

		// Ensures RAM placeholder is large enough to accommodate the compressed archive
		if int64(len(ramdiskBytes)) > m.isoInfo.rdLengthBytes {
			return errors.Errorf("custom RAM disk is too long to be embedded (%d bytes > %d bytes)", len(ramdiskBytes), m.isoInfo.rdLengthBytes)
		}

		if err := m.uploadCustomPart(m.origRdContents, ramdiskBytes, rdPartNum); err != nil {
			return errors.Wrapf(err, "failed to upload ramdisk for file %s", m.destObjectKey)
		}
	}

	return nil
}

func (m *multiUpload) uploadCustomPart(origContents *[]byte, customBytes []byte, partNum int64) error {
	copy(*origContents, customBytes)

	contentLength := int64(len(*origContents))
	completedPartCopy, err := m.uploader.s3client.UploadPart(&s3.UploadPartInput{
		Bucket:        aws.String(m.uploader.bucket),
		Key:           aws.String(m.destObjectKey),
		PartNumber:    aws.Int64(partNum),
		UploadId:      aws.String(m.uploadID),
		Body:          bytes.NewReader(*origContents),
		ContentLength: aws.Int64(contentLength),
	})
	if err != nil {
		return err
	}

	m.appendCompletedPart(&s3.CompletedPart{ETag: completedPartCopy.ETag, PartNumber: aws.Int64(partNum)})
	return nil
}
