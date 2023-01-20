//
// Copyright 2020-2022 ArangoDB GmbH, Cologne, Germany
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

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/arangodb/go-driver"

	"github.com/arangodb/arangosync-client/tasks"
)

// InternalMasterAPI contains the internal API of the sync master.
type InternalMasterAPI interface {
	// Worker -> Master

	// Load configuration data from the master
	ConfigureWorker(ctx context.Context, endpoint string) (WorkerConfiguration, error)
	// Return all registered workers
	RegisteredWorkers(ctx context.Context) ([]WorkerRegistration, error)
	// Return info about a specific worker
	RegisteredWorker(ctx context.Context, id string) (WorkerRegistration, error)
	// RegisterWorker registers or updates a worker.
	RegisterWorker(ctx context.Context, endpoint, token, hostID, dbServerAffinity string) (WorkerRegistrationResponse, error)
	// Remove the registration of a worker
	UnregisterWorker(ctx context.Context, id string) error
	// Get info about a specific task
	Task(ctx context.Context, id string) (TaskInfo, error)
	// Get all known tasks
	Tasks(ctx context.Context) ([]TaskInfo, error)
	// Get all known tasks for a given channel
	TasksByChannel(ctx context.Context, channelName string) ([]TaskInfo, error)
	// Notify the master that a task with given ID has completed.
	TaskCompleted(ctx context.Context, taskID string, info TaskCompletedRequest) error
	// Create tasks to start synchronization of a shard in the given db+col.
	SynchronizeShard(ctx context.Context, dbName, colName string, shardIndex int) error
	// Stop tasks to synchronize a shard in the given db+col.
	CancelSynchronizeShard(ctx context.Context, dbName, colName string, shardIndex int) error
	// Report status of the synchronization of a shard back to the master.
	SynchronizeShardStatus(ctx context.Context, entries []SynchronizationShardStatusRequestEntry) error
	// UpdateIncomingSynchronizationStatus updates details of synchronization status at master.
	UpdateIncomingSynchronizationStatus(ctx context.Context, update UpdateIncomingSynchronizationStatusRequest) error
	// IsChannelRelevant checks if a MQ channel is still relevant
	IsChannelRelevant(ctx context.Context, channelName string) (bool, error)

	// Worker & Master -> Master

	// GetDirectMQTopicEndpoint returns an endpoint which can be used to fetch direct MQ messages from.
	// This method requires a directMQ token or client cert for authentication.
	GetDirectMQTopicEndpoint(ctx context.Context, channelName, db, col string, shard uint) (DirectMQTopicEndpoint, error)
	// RenewDirectMQToken renews a given direct MQ token.
	// This method requires a directMQ token for authentication.
	RenewDirectMQToken(ctx context.Context, token string) (DirectMQToken, error)
	// CloneDirectMQToken creates a clone of a given direct MQ token.
	// When the given token is revoked, the newly cloned token is also revoked.
	// This method requires a directMQ token for authentication.
	CloneDirectMQToken(ctx context.Context, token string) (DirectMQToken, error)
	// Add entire direct MQ API
	InternalDirectMQAPI

	// Master -> Master

	// OutgoingSynchronization starts a task that sends inventory data to a receiving remote cluster.
	OutgoingSynchronization(ctx context.Context, input OutgoingSynchronizationRequest) (OutgoingSynchronizationResponse, error)
	// CancelOutgoingSynchronization cancels synchronization data to the remote cluster with given ID.
	CancelOutgoingSynchronization(ctx context.Context, remoteID string, serverMode driver.ServerMode) error
	// OutgoingSynchronizeShard creates task to send synchronization data of a shard in the given db+col to a remote cluster.
	OutgoingSynchronizeShard(ctx context.Context, remoteID, dbName, colName string, shardIndex int, input OutgoingSynchronizeShardRequest) error
	// CancelOutgoingSynchronizeShard stops task to send synchronization data of a shard in the given db+col to a remote cluster.
	CancelOutgoingSynchronizeShard(ctx context.Context, remoteID, dbName, colName string, shardIndex int) error
	// OutgoingSynchronizeShardStatus reports status of the synchronization of a shard back to the master.
	OutgoingSynchronizeShardStatus(ctx context.Context, entries []SynchronizationShardStatusRequestEntry) error
	// OutgoingResetShardSynchronization removes and creates new task for the given shard data.
	OutgoingResetShardSynchronization(ctx context.Context, remoteID, dbName, colName string, shardIndex int, newControlChannel, newDataChannel string) error
	// CreateOutgoingSynchronizationBarrier creates a barrier in the outgoing
	// synchronization to the remote cluster with the given ID.
	CreateOutgoingSynchronizationBarrier(ctx context.Context, remoteID string) error
	// CancelOutgoingSynchronizationBarrier removes the active barrier in the
	// current synchronization to the remote cluster with given ID.
	CancelOutgoingSynchronizationBarrier(ctx context.Context, remoteID string) error
	// ChannelPrefix gets a prefix for names of channels that contain message going to this master.
	ChannelPrefix(ctx context.Context) (string, error)
	// CreateMessageQueueConfig creates an MQ configuration.
	CreateMessageQueueConfig(ctx context.Context) (MessageQueueConfigExt, error)
	// UpdateMessageQueue updates an MQ configuration.
	UpdateMessageQueue(ctx context.Context, input UpdateMQConfigRequest) error
}

// InternalWorkerAPI contains the internal API of the sync worker.
type InternalWorkerAPI interface {
	// StartTask is called by the master to instruct the worker
	// to run a task with given instructions.
	StartTask(ctx context.Context, data StartTaskRequest) error
	// StopTask is called by the master to instruct the worker
	// to stop all work on the given task.
	StopTask(ctx context.Context, taskID string) error
	// Configure is called by the master to instruct the worker that configuration has been changed.
	Configure(ctx context.Context, JWTSecret string) error
	// SetDirectMQTopicToken configures the token used to access messages of a given channel.
	SetDirectMQTopicToken(ctx context.Context, channelName, token string, tokenTTL time.Duration) error
	// InternalDirectMQAPI adds entire direct MQ API.
	InternalDirectMQAPI
}

// InternalDirectMQAPI contains the internal API of the sync master/worker wrt direct MQ messages.
type InternalDirectMQAPI interface {
	// GetDirectMQMessages return messages for a given MQ channel.
	GetDirectMQMessages(ctx context.Context, channelName string) ([]DirectMQMessage, error)
	// CommitDirectMQMessage removes all messages from the given channel up to an including the given offset.
	CommitDirectMQMessage(ctx context.Context, channelName string, offset int64) error
}

// MessageQueueConfig contains all deployment configuration info for the local MQ.
type MessageQueueConfig struct {
	Endpoints      Endpoint          `json:"endpoints"`
	Authentication TLSAuthentication `json:"authentication"`
}

// MessageQueueConfigExt contains extended MQ config.
type MessageQueueConfigExt struct {
	MessageQueueConfig
	// TokenTTL holds how long MQ config token is valid.
	TokenTTL time.Duration `json:"token-ttl,omitempty"`
}

// Clone returns a deep copy of the given config
func (c MessageQueueConfig) Clone() MessageQueueConfig {
	result := c
	result.Endpoints = append([]string{}, c.Endpoints...)
	return result
}

// Equals checks if the structures are the same.
func (c MessageQueueConfig) Equals(other MessageQueueConfig) bool {
	return c.Authentication.Equals(other.Authentication) &&
		c.Endpoints.EqualsOrder(other.Endpoints)
}

// ConfigureWorkerRequest is the JSON body for the ConfigureWorker request.
type ConfigureWorkerRequest struct {
	Endpoint string `json:"endpoint"` // Endpoint of the worker
}

// WorkerConfiguration contains configuration data passed from
// the master to the worker.
type WorkerConfiguration struct {
	Cluster struct {
		Endpoints       []string `json:"endpoints"`
		JWTSecret       string   `json:"jwtSecret,omitempty"`
		MaxDocumentSize int      `json:"maxDocumentSize,omitempty"`
		// Minimum replication factor of new/modified collections
		MinReplicationFactor int `json:"min-replication-factor,omitempty"`
		// Maximum replication factor of new/modified collections
		MaxReplicationFactor int  `json:"max-replication-factor,omitempty"`
		ExcludeRootUser      bool `json:"exclude-root-user,omitempty"`
	} `json:"cluster"`
	HTTPServer struct {
		Certificate string `json:"certificate"`
		Key         string `json:"key"`
	} `json:"httpServer"`
	MessageQueue struct {
		MessageQueueConfig // MQ configuration of local MQ
	} `json:"mq"`
}

// SetDefaults fills empty values with defaults
func (c *WorkerConfiguration) SetDefaults() {
	if c.Cluster.MinReplicationFactor <= 0 {
		c.Cluster.MinReplicationFactor = 1
	}
	if c.Cluster.MaxReplicationFactor <= 0 {
		c.Cluster.MaxReplicationFactor = math.MaxInt32
	}
}

// Validate the given configuration.
// Return an error on validation errors, nil when all ok.
func (c WorkerConfiguration) Validate() error {
	if c.Cluster.MinReplicationFactor < 1 {
		return maskAny(fmt.Errorf("MinReplicationFactor must be >= 1"))
	}
	if c.Cluster.MaxReplicationFactor < 1 {
		return maskAny(fmt.Errorf("MaxReplicationFactor must be >= 1"))
	}
	if c.Cluster.MaxReplicationFactor < c.Cluster.MinReplicationFactor {
		return maskAny(fmt.Errorf("MaxReplicationFactor must be >= MinReplicationFactor"))
	}
	return nil
}

type WorkerRegistrations struct {
	Workers []WorkerRegistration `json:"workers"`
}

type WorkerRegistration struct {
	// ID of the worker assigned to it by the master
	ID string `json:"id"`
	// Endpoint of the worker
	Endpoint string `json:"endpoint"`
	// Expiration time of the last registration of the worker
	ExpiresAt time.Time `json:"expiresAt"`
	// ID of the worker when communicating with ArangoDB servers.
	ServerID int64 `json:"serverID"`
	// HostID is the host ID the worker process is running on. It can be empty.
	HostID string `json:"host,omitempty"`
	// DBServerAffinity indicates where (which DB server) the worker should follow the WAL.
	DBServerAffinity string `json:"dbServerAffinity,omitempty"`
}

// Validate the given registration.
// Return nil if ok, error otherwise.
func (wr WorkerRegistration) Validate() error {
	if wr.ID == "" {
		return maskAny(fmt.Errorf("ID empty"))
	}
	if wr.Endpoint == "" {
		return maskAny(fmt.Errorf("Endpoint empty"))
	}
	if wr.ServerID == 0 {
		return maskAny(fmt.Errorf("ServerID == 0"))
	}
	return nil
}

// IsExpired returns true when the given worker is expired.
func (wr WorkerRegistration) IsExpired() bool {
	return time.Now().After(wr.ExpiresAt)
}

type WorkerRegistrationRequest struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token,omitempty"`
	// HostID is the host ID the worker process is running on. It can be empty.
	// Since version 2.6.0 it used to be invalid tag `json:host,omitempty"` (without `"` before `host`),
	// so the field HostID was taken as a tag, and it must be used to keep backward compatibility.
	HostID string `json:"HostID,omitempty"`
	// DBServerAffinity describes which DB server is accessible from the worker.
	DBServerAffinity string `json:"dbServerAffinity,omitempty"`
}

type WorkerRegistrationResponse struct {
	WorkerRegistration
	// Maximum time between message in a task channel.
	MessageTimeout  time.Duration `json:"messageTimeout,omitempty"`
	ExpiresDuration time.Duration `json:"expiresDuration,omitempty"`
}

type StartTaskRequest struct {
	ID string `json:"id"`
	tasks.TaskData
	// MQ configuration of the remote cluster
	RemoteMessageQueueConfig MessageQueueConfig `json:"remote-mq-config"`
}

type OutgoingChannels struct {
	// Name of MQ topic to send inventory data to.
	Inventory string `json:"inventory"`
}

// OutgoingSynchronizationRequest holds the master->master request
// data for configuring an outgoing inventory stream.
type OutgoingSynchronizationRequest struct {
	ID                 string             `json:"id"`       // ID of remote cluster
	Target             Endpoint           `json:"target"`   // Endpoints of sync masters of the remote (target) cluster
	Channels           OutgoingChannels   `json:"channels"` // MQ configuration of the remote (target) cluster
	MessageQueueConfig MessageQueueConfig `json:"mq-config"`
}

// Clone returns a deep copy of the given request.
func (r OutgoingSynchronizationRequest) Clone() OutgoingSynchronizationRequest {
	c := r
	c.Target = r.Target.Clone()
	c.MessageQueueConfig = r.MessageQueueConfig.Clone()
	return c
}

// OutgoingSynchronizationResponse holds the answer to an
// master->master request for configuring an outgoing synchronization.
type OutgoingSynchronizationResponse struct {
	// MQ configuration of the remote (source) cluster
	MessageQueueConfig MessageQueueConfig `json:"mq-config"`
}

type Channels struct {
	Control string `json:"control"` // Name of MQ topic to receive control messages on.
	Data    string `json:"data"`    // Name of MQ topic to send data messages to.
}

// OutgoingSynchronizeShardRequest holds the master->master request
// data for configuring an outgoing shard synchronization stream.
type OutgoingSynchronizeShardRequest struct {
	Channels Channels `json:"channels"`
}

// SynchronizationShardStatusRequest is the request body of a (Outgoing)SynchronizationStatus request.
type SynchronizationShardStatusRequest struct {
	Entries []SynchronizationShardStatusRequestEntry `json:"entries"`
}

// SynchronizationShardStatusRequestEntry is a single entry in a SynchronizationShardStatusRequest
type SynchronizationShardStatusRequestEntry struct {
	RemoteID   string                `json:"remoteID"`
	Database   string                `json:"database"`
	Collection string                `json:"collection"`
	ShardIndex int                   `json:"shardIndex"`
	Status     SynchronizationStatus `json:"status"`
}

// UpdateIncomingSynchronizationStatusRequest is the request body for UpdateIncomingSynchronizationStatus request
type UpdateIncomingSynchronizationStatusRequest struct {
	TotalNumberOfShards int `json:"totalNumberOfShards"`
}

type SynchronizationStatus struct {
	// Current status
	Status SyncStatus `json:"status"`
	// Human readable status message
	StatusMessage string `json:"status_message,omitempty"`
	// Delay between us and other data center.
	Delay time.Duration `json:"delay"`
	// Time of last message received by the task handling this shard
	LastMessage time.Time `json:"last_message"`
	// Time of last message that resulted in a data change, received by the task handling this shard
	LastDataChange time.Time `json:"last_data_change"`
	// Time of when we last had a change in the status of the shard master
	LastShardMasterChange time.Time `json:"last_shard_master_change"`
	// Is the shard master known?
	ShardMasterKnown bool `json:"shard_master_known"`
	// Is the shard known to be in-sync?
	// This requires the sending side to be in read-only mode.
	InSync bool `json:"in_sync"`
}

// IsSame returns true when the Status & StatusMessage of both statuses
// are equal and the Delay is very close.
func (s SynchronizationStatus) IsSame(other SynchronizationStatus) bool {
	if s.Status != other.Status || s.StatusMessage != other.StatusMessage ||
		s.LastMessage != other.LastMessage || s.LastDataChange != other.LastDataChange ||
		s.LastShardMasterChange != other.LastShardMasterChange || s.ShardMasterKnown != other.ShardMasterKnown || s.InSync != other.InSync {
		return false
	}
	return !IsSignificantDelayDiff(s.Delay, other.Delay)
}

// TaskCompletedRequest holds the info for a TaskCompleted request.
type TaskCompletedRequest struct {
	// Error is set when task finished with error.
	Error bool `json:"error,omitempty"`
}

// TaskAssignment contains information of the assignment of a
// task to a worker.
// It is serialized as JSON into the agency.
type TaskAssignment struct {
	// ID of worker the task is assigned to
	WorkerID string `json:"worker_id"`
	// When the assignment was made
	CreatedAt time.Time `json:"created_at"`
	// How many assignments have been made
	Counter int `json:"counter,omitempty"`
}

// TaskInfo contains all information known about a task.
type TaskInfo struct {
	ID         string         `json:"id"`
	Task       tasks.TaskData `json:"task"`
	Assignment TaskAssignment `json:"assignment"`
}

// IsAssigned returns true when the task in given info is assigned to a
// worker, false otherwise.
func (i TaskInfo) IsAssigned() bool {
	return i.Assignment.WorkerID != ""
}

// TasksResponse is the JSON response for MasterAPI.Tasks method.
type TasksResponse struct {
	Tasks []TaskInfo `json:"tasks,omitempty"`
}

// IsSignificantDelayDiff returns true if there is a significant difference
// between the given delays.
func IsSignificantDelayDiff(d1, d2 time.Duration) bool {
	if d2 == 0 {
		return d1 != 0
	}
	x := float64(d1) / float64(d2)
	return x < 0.9 || x > 1.1
}

// IsChannelRelevantResponse is the JSON response for a MasterAPI.IsChannelRelevant call
type IsChannelRelevantResponse struct {
	IsRelevant bool `json:"isRelevant"`
}

// StatusAPI describes the API provided to task workers used to send status updates to the master.
type StatusAPI interface {
	// SendIncomingStatus queues a given incoming synchronization status entry for sending.
	SendIncomingStatus(ctx context.Context, entry SynchronizationShardStatusRequestEntry) error
	// SendOutgoingStatus queues a given outgoing synchronization status entry for sending.
	SendOutgoingStatus(ctx context.Context, entry SynchronizationShardStatusRequestEntry) error
	// UpdateSynchronizationStatus queues a request to update total number of shards that should be synchronized from source cluster.
	UpdateSynchronizationStatus(ctx context.Context, req UpdateIncomingSynchronizationStatusRequest) error
}

// DirectMQToken provides a token with its TTL
type DirectMQToken struct {
	// Token used to authenticate with the server.
	Token string `json:"token"`
	// How long the token will be valid.
	// Afterwards a new token has to be fetched.
	TokenTTL time.Duration `json:"token-ttl"`
}

// DirectMQTokenRequest is the JSON request body for Renew/Clone direct MQ token request.
type DirectMQTokenRequest struct {
	// Token used to authenticate with the server.
	Token string `json:"token"`
}

// DirectMQTopicEndpoint provides information about an endpoint for Direct MQ messages.
type DirectMQTopicEndpoint struct {
	// Endpoint of the server that can provide messages for a specific topic.
	Endpoint Endpoint `json:"endpoint"`
	// CA certificate used to sign the TLS connection of the server.
	// This is used for verifying the server.
	CACertificate string `json:"caCertificate"`
	// Token used to authenticate with the server.
	Token string `json:"token"`
	// How long the token will be valid.
	// Afterwards a new token has to be fetched.
	TokenTTL time.Duration `json:"token-ttl"`
}

// SetDirectMQTopicTokenRequest is the JSON request body for SetDirectMQTopicToken request.
type SetDirectMQTopicTokenRequest struct {
	// Token used to authenticate with the server.
	Token string `json:"token"`
	// How long the token will be valid.
	// Afterwards a new token has to be fetched.
	TokenTTL time.Duration `json:"token-ttl"`
}

// ConfigurationRequest is the JSON request body for changing configuration.
type ConfigurationRequest struct {
	JWTSecret string `json:"secret"` // JWT secret used to create token for authentication with the server.
}

// DirectMQMessage is a direct MQ message.
type DirectMQMessage struct {
	Offset  int64           `json:"offset"`
	Message json.RawMessage `json:"message"`
}

// GetDirectMQMessagesResponse is the JSON body for GetDirectMQMessages response.
type GetDirectMQMessagesResponse struct {
	Messages []DirectMQMessage `json:"messages,omitempty"`
}

// CommitDirectMQMessageRequest is the JSON request body for CommitDirectMQMessage request.
type CommitDirectMQMessageRequest struct {
	Offset int64 `json:"offset"`
}

func (c Channels) IsEqual(compareTo Channels) bool {
	return c.Data == compareTo.Data && c.Control == compareTo.Control
}

// UpdateMQConfigRequest describes what is required to update a message queue config.
type UpdateMQConfigRequest struct {
	ID       string        `json:"id"`        // ID of remote cluster.
	Token    string        `json:"token"`     // A new token which should be updated remotely.
	TokenTTL time.Duration `json:"token-ttl"` // TTL of the token.
}
