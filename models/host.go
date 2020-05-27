// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// Host host
//
// swagger:model host
type Host struct {

	// bootstrap
	Bootstrap bool `json:"bootstrap,omitempty"`

	// checked in at
	// Format: date-time
	CheckedInAt strfmt.DateTime `json:"checked_in_at,omitempty" gorm:"type:datetime"`

	// The cluster that this host is associated with.
	// Format: uuid
	ClusterID strfmt.UUID `json:"cluster_id,omitempty" gorm:"primary_key;foreignkey:Cluster"`

	// connectivity
	Connectivity string `json:"connectivity,omitempty" gorm:"type:text"`

	// created at
	// Format: date-time
	CreatedAt strfmt.DateTime `json:"created_at,omitempty" gorm:"type:datetime"`

	// hardware info
	HardwareInfo string `json:"hardware_info,omitempty" gorm:"type:text"`

	// Self link.
	// Required: true
	Href *string `json:"href"`

	// Unique identifier of the object.
	// Required: true
	// Format: uuid
	ID *strfmt.UUID `json:"id" gorm:"primary_key"`

	// inventory
	Inventory string `json:"inventory,omitempty" gorm:"type:text"`

	// Indicates the type of this object. Will be 'Host' if this is a complete object or 'HostLink' if it is just a link.
	// Required: true
	// Enum: [Host]
	Kind *string `json:"kind"`

	// role
	// Enum: [undefined master worker]
	Role string `json:"role,omitempty"`

	// status
	// Required: true
	// Enum: [discovering known disconnected insufficient disabled installing installing-in-progress installed error]
	Status *string `json:"status"`

	// status info
	// Required: true
	StatusInfo *string `json:"status_info" gorm:"type:varchar(2048)"`

	// The last time that the host status has been updated
	// Required: true
	// Format: date-time
	StatusUpdatedAt *strfmt.DateTime `json:"status_updated_at" gorm:"type:datetime"`

	// updated at
	// Format: date-time
	UpdatedAt strfmt.DateTime `json:"updated_at,omitempty" gorm:"type:datetime"`
}

// Validate validates this host
func (m *Host) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCheckedInAt(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateClusterID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateCreatedAt(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateHref(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateKind(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateRole(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateStatusInfo(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateStatusUpdatedAt(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateUpdatedAt(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Host) validateCheckedInAt(formats strfmt.Registry) error {

	if swag.IsZero(m.CheckedInAt) { // not required
		return nil
	}

	if err := validate.FormatOf("checked_in_at", "body", "date-time", m.CheckedInAt.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *Host) validateClusterID(formats strfmt.Registry) error {

	if swag.IsZero(m.ClusterID) { // not required
		return nil
	}

	if err := validate.FormatOf("cluster_id", "body", "uuid", m.ClusterID.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *Host) validateCreatedAt(formats strfmt.Registry) error {

	if swag.IsZero(m.CreatedAt) { // not required
		return nil
	}

	if err := validate.FormatOf("created_at", "body", "date-time", m.CreatedAt.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *Host) validateHref(formats strfmt.Registry) error {

	if err := validate.Required("href", "body", m.Href); err != nil {
		return err
	}

	return nil
}

func (m *Host) validateID(formats strfmt.Registry) error {

	if err := validate.Required("id", "body", m.ID); err != nil {
		return err
	}

	if err := validate.FormatOf("id", "body", "uuid", m.ID.String(), formats); err != nil {
		return err
	}

	return nil
}

var hostTypeKindPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["Host"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		hostTypeKindPropEnum = append(hostTypeKindPropEnum, v)
	}
}

const (

	// HostKindHost captures enum value "Host"
	HostKindHost string = "Host"
)

// prop value enum
func (m *Host) validateKindEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, hostTypeKindPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *Host) validateKind(formats strfmt.Registry) error {

	if err := validate.Required("kind", "body", m.Kind); err != nil {
		return err
	}

	// value enum
	if err := m.validateKindEnum("kind", "body", *m.Kind); err != nil {
		return err
	}

	return nil
}

var hostTypeRolePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["undefined","master","worker"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		hostTypeRolePropEnum = append(hostTypeRolePropEnum, v)
	}
}

const (

	// HostRoleUndefined captures enum value "undefined"
	HostRoleUndefined string = "undefined"

	// HostRoleMaster captures enum value "master"
	HostRoleMaster string = "master"

	// HostRoleWorker captures enum value "worker"
	HostRoleWorker string = "worker"
)

// prop value enum
func (m *Host) validateRoleEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, hostTypeRolePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *Host) validateRole(formats strfmt.Registry) error {

	if swag.IsZero(m.Role) { // not required
		return nil
	}

	// value enum
	if err := m.validateRoleEnum("role", "body", m.Role); err != nil {
		return err
	}

	return nil
}

var hostTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["discovering","known","disconnected","insufficient","disabled","installing","installing-in-progress","installed","error"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		hostTypeStatusPropEnum = append(hostTypeStatusPropEnum, v)
	}
}

const (

	// HostStatusDiscovering captures enum value "discovering"
	HostStatusDiscovering string = "discovering"

	// HostStatusKnown captures enum value "known"
	HostStatusKnown string = "known"

	// HostStatusDisconnected captures enum value "disconnected"
	HostStatusDisconnected string = "disconnected"

	// HostStatusInsufficient captures enum value "insufficient"
	HostStatusInsufficient string = "insufficient"

	// HostStatusDisabled captures enum value "disabled"
	HostStatusDisabled string = "disabled"

	// HostStatusInstalling captures enum value "installing"
	HostStatusInstalling string = "installing"

	// HostStatusInstallingInProgress captures enum value "installing-in-progress"
	HostStatusInstallingInProgress string = "installing-in-progress"

	// HostStatusInstalled captures enum value "installed"
	HostStatusInstalled string = "installed"

	// HostStatusError captures enum value "error"
	HostStatusError string = "error"
)

// prop value enum
func (m *Host) validateStatusEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, hostTypeStatusPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *Host) validateStatus(formats strfmt.Registry) error {

	if err := validate.Required("status", "body", m.Status); err != nil {
		return err
	}

	// value enum
	if err := m.validateStatusEnum("status", "body", *m.Status); err != nil {
		return err
	}

	return nil
}

func (m *Host) validateStatusInfo(formats strfmt.Registry) error {

	if err := validate.Required("status_info", "body", m.StatusInfo); err != nil {
		return err
	}

	return nil
}

func (m *Host) validateStatusUpdatedAt(formats strfmt.Registry) error {

	if err := validate.Required("status_updated_at", "body", m.StatusUpdatedAt); err != nil {
		return err
	}

	if err := validate.FormatOf("status_updated_at", "body", "date-time", m.StatusUpdatedAt.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *Host) validateUpdatedAt(formats strfmt.Registry) error {

	if swag.IsZero(m.UpdatedAt) { // not required
		return nil
	}

	if err := validate.FormatOf("updated_at", "body", "date-time", m.UpdatedAt.String(), formats); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *Host) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Host) UnmarshalBinary(b []byte) error {
	var res Host
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
