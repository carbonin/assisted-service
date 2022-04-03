package hostcommands

import (
	"context"


	"github.com/go-openapi/swag"
	"github.com/openshift/assisted-service/internal/common"
	"github.com/openshift/assisted-service/internal/host/hostutil"
	"github.com/openshift/assisted-service/models"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type tangConnectivityCheckCmd struct {
	baseCmd
	db                         *gorm.DB
	tangConnectivityCheckImage string
}

func NewTangConnectivityCheckCmd(log logrus.FieldLogger, db *gorm.DB, tangConnectivityCheckImage string) *tangConnectivityCheckCmd {
	return &tangConnectivityCheckCmd{
		baseCmd:                    baseCmd{log: log},
		db:                         db,
		tangConnectivityCheckImage: tangConnectivityCheckImage,
	}
}

func (c *tangConnectivityCheckCmd) GetSteps(ctx context.Context, host *models.Host) ([]*models.Step, error) {
	var cluster common.Cluster

	if hostutil.IsDay2Host(host) {
		c.log.Debugf("skipping tangConnectivityCheck for day2 host %s", host.ID.String())
		return []*models.Step{}, nil
	}

	if err := c.db.First(&cluster, "id = ?", host.ClusterID).Error; err != nil {
		c.log.WithError(err).Errorf("failed to fetch cluster %s", host.ClusterID)
		return nil, err
	}

	if cluster.DiskEncryption == nil ||
		swag.StringValue(cluster.DiskEncryption.EnableOn) == models.DiskEncryptionEnableOnNone ||
		swag.StringValue(cluster.DiskEncryption.Mode) == models.DiskEncryptionModeTpmv2 ||
		!hostutil.IsDiskEncryptionEnabledForRole(*cluster.DiskEncryption, common.GetEffectiveRole(host)) {
		c.log.Debugf("skipping tangConnectivityCheck for host %s, DiskEncryption config do not require validation here", host.ID.String())
		return []*models.Step{}, nil
	}

	step := &models.Step{
		StepType: models.StepTypeTangConnectivityCheck,
		Command:  "podman",
		Args: []string{
			"run", "--privileged", "--net=host", "--rm", "--quiet",
			"-v", "/var/log:/var/log",
			"-v", "/run/systemd/journal/socket:/run/systemd/journal/socket",
			c.tangConnectivityCheckImage,
			"tang_connectivity_check",
			cluster.DiskEncryption.TangServers,
		},
	}
	return []*models.Step{step}, nil
}
