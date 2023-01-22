//
// Copyright 2017-2022 ArangoDB GmbH, Cologne, Germany
//
// The Programs (which include both the software and documentation) contain
// proprietary information of ArangoDB GmbH; they are provided under a license
// agreement containing restrictions on use and disclosure and are also
// protected by copyright, patent and other intellectual and industrial
// property laws. Reverse engineering, disassembly or decompilation of the
// Programs, except to the extent required to obtain interoperability with
// other independently created software or as specified by law, is prohibited.
//
// It shall be the licensee's responsibility to take all appropriate fail-safe,
// backup, redundancy, and other measures to ensure the safe use of
// applications if the Programs are used for purposes such as nuclear,
// aviation, mass transit, medical, or other inherently dangerous applications,
// and ArangoDB GmbH disclaims liability for any damages caused by such use of
// the Programs.
//
// This software is the confidential and proprietary information of ArangoDB
// GmbH. You shall not disclose such confidential and proprietary information
// and shall use it only in accordance with the terms of the license agreement
// you entered into with ArangoDB GmbH.
//

package tasks

import (
	"context"
	"reflect"
	"time"

	"github.com/rs/zerolog"
)

// TaskController is an interface which describes how to control a task.
type TaskController interface {
	// Validate checks if the task is still valid.
	Validate(ctx context.Context, taskID string) error
	// ResetTask resets a task.
	ResetTask(ctx context.Context, dbName, col string, shardIndex int) error
	// GetMessageTimeout gets message timeout for a task.
	GetMessageTimeout() time.Duration
	// AddTaskToInitialSyncQueue returns true if task has been added to an initial-sync queue.
	AddTaskToInitialSyncQueue(taskID string) bool
	// RemoveTaskFromInitialSyncQueue informs task controller about initial-sync completion phase.
	RemoveTaskFromInitialSyncQueue(taskID string)
}

// TaskData contains persistent data of a task.
// This data is stored as JSON object in the agency.
type TaskData struct {
	// Type of task
	Type TaskType `json:"type"`
	// If Persistent is set, this task should be re-assigned to another worker when
	// the worker, that the task was assigned to, is unregistered (or expires).
	Persistent bool `json:"persistent,omitempty"`
	// Channels contains names of MQ channels used for this task
	Channels struct {
		// Data channel is used to send data messages from sync source to sync target
		Data string `json:"data,omitempty"`
		// Control channel is used to send control messages from sync target to sync source.
		Control string `json:"control,omitempty"`
	} `json:"channels"`
	// If set, contains the ID of the remote cluster this task is targeting
	TargetID string `json:"target_id,omitempty"`
	// If set, contains the name of the database this task is working on.
	Database string `json:"database,omitempty"`
	// If set, contains the name of the collection this task is working on.
	Collection string `json:"collection,omitempty"`
	// If set, contains the index of the shard this task is working on.
	ShardIndex int `json:"shardIndex,omitempty"`
}

// IsShardSpecific returns true when the task is intended to operate on a specific shard.
// The persistent task is not the specific shard task.
func (t TaskData) IsShardSpecific() bool {
	return t.Database != "" && t.Collection != ""
}

// Equals returns true when both TaskData's are identical.
func (t TaskData) Equals(other TaskData) bool {
	return reflect.DeepEqual(t, other)
}

// TaskType is a type of task.
// Values are hardcoded and should not be changed.
type TaskType string

const (
	// TaskTypeSendInventory is a task type that sends inventory updates to the sync target.
	TaskTypeSendInventory TaskType = "send-inventory"
	// TaskTypeReceiveInventory is a task type that received inventory updates from the sync source and updates the local
	// structure accordingly.
	TaskTypeReceiveInventory TaskType = "receive-inventory"
	// TaskTypeSendShard is a task type that sends synchronization updates to the sync target for a specific shard.
	TaskTypeSendShard TaskType = "send-shard"
	// TaskTypeReceiveShard is a task type that received synchronization updates from the sync source for a specific shard.
	TaskTypeReceiveShard TaskType = "receive-shard"
)

func (t TaskType) String() string {
	return string(t)
}

// TaskCleaner is a task's cleaner interface.
type TaskCleaner interface {
	// CleanUp cleans up a task when it finishes.
	CleanUp(ctx context.Context) error
}

// TaskWorker is a generic interface for the implementation of a task.
type TaskWorker interface {
	TaskCleaner
	// Run the task.
	// Do not return until completion or a fatal error occurs
	Run() error

	// Stop the task.
	// If waitUntilFinished is set, do not return until the task has been stopped.
	Stop(waitUntilFinished bool)

	// RenewTokens is called once every 5 minutes. The task worker is expected to renew all
	// authentication tokens it needs.
	RenewTokens(ctx context.Context) error
}

// TLSClientAuthentication contains configuration for using client certificates or client tokens.
// If new field is added then the function `Equals` must be changed if it is necessary.
type TLSClientAuthentication struct {
	// Client certificate used to authenticate myself.
	ClientCertificate string `json:"clientCertificate"`
	// Private key of client certificate used to authentication.
	ClientKey string `json:"clientKey"`
	// Client token used to authenticate myself.
	ClientToken string `json:"clientToken"`
}

// String returns a string representation of the given object.
func (a TLSClientAuthentication) String() string {
	return a.ClientCertificate + "/" + a.ClientKey + "/" + a.ClientToken
}

// Equals returns true if the structures are the same.
func (a TLSClientAuthentication) Equals(other TLSClientAuthentication) bool {
	return a == other
}

// TLSAuthentication contains configuration for using client certificates
// and TLS verification of the server.
type TLSAuthentication struct {
	TLSClientAuthentication
	// CA certificate used to sign the TLS connection of the server.
	// This is used for verifying the server.
	CACertificate string `json:"caCertificate"`
}

// String returns a string representation of the given object.
func (a TLSAuthentication) String() string {
	return a.TLSClientAuthentication.String() + "/" + a.CACertificate
}

// Equals returns true if the structures are the same.
// If new field is added then the function `Equals` must be changed if it is necessary.
func (a TLSAuthentication) Equals(other TLSAuthentication) bool {
	return a.CACertificate == other.CACertificate &&
		a.TLSClientAuthentication == other.TLSClientAuthentication
}

// MessageQueueConfig contains all deployment configuration info for a MQ.
type MessageQueueConfig struct {
	Type           string            `json:"type"`
	Endpoints      []string          `json:"endpoints"`
	Authentication TLSAuthentication `json:"authentication"`
}

// CommonConfig contains the parameters to be used by all workers.
type CommonConfig struct {
	TaskID              string
	TaskData            *TaskData
	TaskController      TaskController
	Log                 zerolog.Logger
	ServerID            int64
	ExcludeRootUserSync bool
}
