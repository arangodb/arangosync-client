//
// Copyright 2017-2021 ArangoDB GmbH, Cologne, Germany
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
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/arangodb/go-driver"

	"github.com/arangodb/arangosync-client/tasks"
)

// API of a sync master/worker
type API interface {
	// Close this client
	Close() error
	// Get the version of the sync master/worker
	Version(ctx context.Context) (VersionInfo, error)
	// Get the role of the sync master/worker
	Role(ctx context.Context) (Role, error)
	// Health performs a quick health check.
	// Returns an error when anything is wrong. If so, check Status.
	Health(ctx context.Context) error
	// Returns the master API (only valid when Role returns master)
	Master() MasterAPI
	// Returns the worker API (only valid when Role returns worker)
	Worker() WorkerAPI
	// SetShared marks the client as shared.
	// Closing a shared client will not close all idle connections.
	SetShared()
	// SynchronizeMasterEndpoints ensures that the client is using all known master
	// endpoints.
	// Do not use for connections to workers.
	// Returns true when endpoints have changed.
	SynchronizeMasterEndpoints(ctx context.Context) (bool, error)
	// Endpoint returns the currently used endpoint for this client.
	Endpoint() Endpoint
}

const (
	// ClientIDHeaderKey is the name of a request header containing the ID that is
	// making the request.
	ClientIDHeaderKey = "X-ArangoSync-Client-ID"
)

// GetSyncStatusDetails specifies what details will be sent in response for sync Status request
type GetSyncStatusDetails string

const (
	// GetSyncStatusDetailsFull will return fully populated SyncInfo
	GetSyncStatusDetailsFull = "full"
	// GetSyncStatusDetailsShort does not return Shards info - only total count
	GetSyncStatusDetailsShort = "short"
)

func (d GetSyncStatusDetails) IsValid() bool {
	switch d {
	case GetSyncStatusDetailsFull, GetSyncStatusDetailsShort:
		return true
	}
	return false
}

// MasterAPI contains API of sync master
type MasterAPI interface {
	// Status Gets the current status of synchronization towards the local cluster.
	Status(ctx context.Context, details GetSyncStatusDetails) (SyncInfo, error)
	// Synchronize configures the master to synchronize the local cluster from a given remote cluster.
	Synchronize(ctx context.Context, input SynchronizationRequest) error
	// CancelSynchronization configure the master to stop & completely cancel the current synchronization of the
	// local cluster from a remote cluster.
	// If this is an active synchronization barrier, this is automatically canceled.
	// Errors:
	// - RequestTimeoutError when input.WaitTimeout is non-zero and the inactive stage is not reached in time.
	CancelSynchronization(ctx context.Context, input CancelSynchronizationRequest) (CancelSynchronizationResponse, error)
	// GetChecksumShardSynchronization gets checksums for the specific shard.
	GetChecksumShardSynchronization(ctx context.Context, dbName, colName string, shardIndex int,
		options ChecksumSynchronizationRequestOptions) (ChecksumSynchronizationResponse, error)
	// GetDataCentersInfo return information from each data center.
	GetDataCentersInfo(ctx context.Context) (DataCentersResponse, error)
	// CreateSynchronizationBarrier creates a barrier in the current synchronization
	// that stops the source cluster from accepting any modifications
	// such that the destination cluster can catch up.
	CreateSynchronizationBarrier(ctx context.Context) error
	// CancelSynchronizationBarrier removes the active barrier in the current synchronization
	// that stops the source cluster from accepting any modifications
	// such that the destination cluster can catch up.
	// If there is not active barrier, this function returns without an error.
	CancelSynchronizationBarrier(ctx context.Context) error
	// GetSynchronizationBarrierStatus returns the status of the the active barrier
	// in the current synchronization that stops the source cluster from accepting any modifications
	// such that the destination cluster can catch up.
	GetSynchronizationBarrierStatus(ctx context.Context) (SynchronizationBarrierStatus, error)
	// ResetShardSynchronization resets failed shard synchronization.
	ResetShardSynchronization(ctx context.Context, dbName, colName string, shardIndex int) error
	// GetMessageTimeout gets current allowed time between messages in a task channel.
	GetMessageTimeout(ctx context.Context) (MessageTimeoutInfo, error)
	// SetMessageTimeout updates the maximum allowed time between messages in a task channel.
	SetMessageTimeout(ctx context.Context, timeout time.Duration) error
	// GetEndpoints returns a list of all known master endpoints of this datacenter.
	// The resulting endpoints are usable from inside and outside the datacenter.
	GetEndpoints(ctx context.Context) (Endpoint, error)
	// GetLeaderEndpoint returns a list of master endpoints of the leader (syncmaster) of this datacenter.
	// Length of returned list will be 1 or the call will fail because no master is available.
	// In the very rare occasion that the leadership is changing during this call, a list
	// of length 0 can be returned.
	// The resulting endpoint is usable only within the same datacenter.
	GetLeaderEndpoint(ctx context.Context) (Endpoint, error)
	// Masters returns a list of known masters in this datacenter.
	Masters(ctx context.Context) ([]MasterInfo, error)

	InternalMasterAPI
}

// WorkerAPI contains API of sync worker
type WorkerAPI interface {
	InternalWorkerAPI
}

type VersionInfo struct {
	Version string `json:"version"`
	Build   string `json:"build"`
}

// MasterInfo contains information about a single master.
type MasterInfo struct {
	// Unique identifier of the master
	ID string `json:"id"`
	// Internal endpoint of the master
	Endpoint string `json:"endpoint"`
	// Is this master the current leader
	Leader bool `json:"leader"`
}

type RoleInfo struct {
	Role Role `json:"role"`
}

type Role string

const (
	RoleMaster Role = "master"
	RoleWorker Role = "worker"
)

func (r Role) IsMaster() bool { return r == RoleMaster }
func (r Role) IsWorker() bool { return r == RoleWorker }

type ChannelPrefixInfo struct {
	Prefix string `json:"prefix"`
}

// SyncInfo holds the JSON info returned from `GET /_api/sync`
type SyncInfo struct {
	// Endpoint of sync master on remote cluster
	Source Endpoint `json:"source"`
	// Overall status of (incoming) synchronization
	Status SyncStatus `json:"status"`
	// Status of incoming synchronization per shard
	Shards []ShardSyncInfo `json:"shards,omitempty"`
	// ShardsCount contains number of shards which have a corresponding synchronization task.
	// This field is always populated, while Shards populated only if GetSyncStatusDetailsFull is provided
	ShardsCount int `json:"shardsCount,omitempty"`
	// TotalShardsCount contains total number of shards which should be synchronized.
	// It equals to the number of "leader shards" on source cluster minus shards excluded from sync.
	TotalShardsCount int `json:"totalShardsCount,omitempty"`
	// Status of outgoing synchronization
	Outgoing []OutgoingSyncInfo `json:"outgoing,omitempty"`
	// CancellationAborted is set to true when it was not possible to cancel synchronization on source DC.
	CancellationAborted bool `json:"cancellationAborted,omitempty"`
}

// OutgoingSyncInfo holds JSON info returned as part of `GET /_api/sync`
// regarding a specific target for outgoing synchronization data.
type OutgoingSyncInfo struct {
	ID            string          `json:"id"`                       // ID of sync master to which data is being send
	Endpoint      Endpoint        `json:"endpoint"`                 // Endpoint of sync masters to which data is being send
	Status        SyncStatus      `json:"status"`                   // Overall status for this outgoing target
	Shards        []ShardSyncInfo `json:"shards,omitempty"`         // Status of outgoing synchronization per shard for this target
	ShardsCount   int             `json:"shardsCount,omitempty"`    // ShardsCount holds the total count of shards
	ActiveBarrier bool            `json:"active_barrier,omitempty"` // Set if this outgoing target has requested a synchronization barrier
}

// ShardSyncInfo holds JSON info returned as part of `GET /_api/sync`
// regarding a specific shard.
type ShardSyncInfo struct {
	Database              string        `json:"database"`                 // Database containing the collection shard
	Collection            string        `json:"collection"`               // Collection containing the shard
	ShardIndex            int           `json:"shardIndex"`               // Index of the shard (0..)
	Status                SyncStatus    `json:"status"`                   // Status of this shard
	StatusMessage         string        `json:"status_message,omitempty"` // Human readable message about the status of this shard
	Delay                 time.Duration `json:"delay,omitempty"`          // Delay between other datacenter and us.
	LastMessage           time.Time     `json:"last_message"`             // Time of last message received by the task handling this shard
	LastDataChange        time.Time     `json:"last_data_change"`         // Time of last message that resulted in a data change, received by the task handling this shard
	LastShardMasterChange time.Time     `json:"last_shard_master_change"` // Time of when we last had a change in the status of the shard master
	ShardMasterKnown      bool          `json:"shard_master_known"`       // Is the shard master known?
	InSync                bool          `json:"in_sync,omitempty"`        // Set if the shard is known to be in-sync. Requires R/O of source cluster.
}

type SyncStatus string

const (
	// SyncStatusInactive indicates that no synchronization is taking place
	SyncStatusInactive SyncStatus = "inactive"
	// SyncStatusInitializing indicates that synchronization tasks are being setup
	SyncStatusInitializing SyncStatus = "initializing"
	// SyncStatusInitialSync indicates that initial synchronization of collections is ongoing
	SyncStatusInitialSync SyncStatus = "initial-sync"
	// SyncStatusRunning indicates that all collections have been initially synchronized
	// and normal transaction synchronization is active.
	SyncStatusRunning SyncStatus = "running"
	// SyncStatusCancelling indicates that the synchronization process is being cancelled.
	SyncStatusCancelling SyncStatus = "cancelling"
	// SyncStatusFailed indicates that the synchronization process has encountered an unrecoverable failure
	SyncStatusFailed SyncStatus = "failed"
)

var (
	// ValidSyncStatusValues is a list of all possible sync status values.
	ValidSyncStatusValues = []SyncStatus{
		SyncStatusInactive,
		SyncStatusInitializing,
		SyncStatusInitialSync,
		SyncStatusRunning,
		SyncStatusCancelling,
		SyncStatusFailed,
	}
)

// Normalize converts an empty status to inactive.
func (s SyncStatus) Normalize() SyncStatus {
	if s == "" {
		return SyncStatusInactive
	}
	return s
}

// Equals returns true when the other status is equal to the given
// status (both normalized).
func (s SyncStatus) Equals(other SyncStatus) bool {
	return s.Normalize() == other.Normalize()
}

// IsInactiveOrEmpty returns true if the given status equals inactive or is empty.
func (s SyncStatus) IsInactiveOrEmpty() bool {
	return s == SyncStatusInactive || s == ""
}

// IsInitialSyncOrRunning returns true if the given status equals initial-sync or running.
func (s SyncStatus) IsInitialSyncOrRunning() bool {
	return s == SyncStatusInitialSync || s == SyncStatusRunning
}

// IsActive returns true if the given status indicates an active state.
// The is: initializing, initial-sync or running
func (s SyncStatus) IsActive() bool {
	return s == SyncStatusInitializing || s == SyncStatusInitialSync || s == SyncStatusRunning
}

// GetShardStatusMessage returns sync status message.
func (s SyncStatus) GetShardStatusMessage() string {
	return fmt.Sprintf("shard's status %s", s)
}

// TLSAuthentication contains configuration for using client certificates
// and TLS verification of the server.
type TLSAuthentication = tasks.TLSAuthentication

type SynchronizationRequest struct {
	// Endpoint of sync master of the source cluster
	Source Endpoint `json:"source"`
	// Authentication of the master
	Authentication TLSAuthentication `json:"authentication"`
}

// Clone returns a deep copy of the given request.
func (r SynchronizationRequest) Clone() SynchronizationRequest {
	c := r
	c.Source = r.Source.Clone()
	return c
}

// IsSame returns true if both requests contain the same values.
// The source is considered the same is the intersection of existing & given source is not empty.
// We consider an intersection because:
// - Servers can be down, resulting in a temporary missing endpoint
// - Customer can specify only 1 of all servers
func (r SynchronizationRequest) IsSame(other SynchronizationRequest) bool {
	if r.Source.Intersection(other.Source).IsEmpty() {
		return false
	}
	if r.Authentication.ClientCertificate != other.Authentication.ClientCertificate {
		return false
	}
	if r.Authentication.ClientKey != other.Authentication.ClientKey {
		return false
	}
	if r.Authentication.CACertificate != other.Authentication.CACertificate {
		return false
	}
	return true
}

// Validate checks the values of the given request and returns an error
// in case of improper values.
// Returns nil on success.
func (r SynchronizationRequest) Validate() error {
	if len(r.Source) == 0 {
		return errors.Wrap(BadRequestError, "source missing")
	}
	if err := r.Source.Validate(); err != nil {
		return errors.Wrapf(BadRequestError, "Invalid source: %s", err.Error())
	}
	if r.Authentication.ClientCertificate == "" {
		return errors.Wrap(BadRequestError, "clientCertificate missing")
	}
	if r.Authentication.ClientKey == "" {
		return errors.Wrap(BadRequestError, "clientKey missing")
	}
	if r.Authentication.CACertificate == "" {
		return errors.Wrap(BadRequestError, "caCertificate missing")
	}
	return nil
}

type CancelSynchronizationRequest struct {
	// Deprecated: WaitTimeout is the amount of time the cancel function will wait
	// until the synchronization has reached an `inactive` state.
	// If this value is zero, the cancel function will only switch to the canceling state
	// but not wait until the `inactive` state is reached.
	// Cancellation request should be sent without this field and the synchronization status
	// should be checked periodically.
	WaitTimeout time.Duration `json:"wait_timeout,omitempty"`
	// Force is set if you want to end the synchronization even if the source
	// master cannot be reached.
	Force bool `json:"force,omitempty"`
	// ForceTimeout is the amount of time the syncmaster tries to contact
	// the source master to notify it about cancelling the synchronization.
	// This fields is only used when Force is true.
	ForceTimeout time.Duration `json:"force_timeout,omitempty"`
	// TargetServerMode is the final target server mode when synchronization is stopped.
	TargetServerMode driver.ServerMode `json:"target_server_mode,omitempty"`
	// SourceServerMode is the final source server mode when synchronization is stopped.
	SourceServerMode driver.ServerMode `json:"source_server_mode,omitempty"`
}

type CancelSynchronizationResponse struct {
	// Aborted is set when synchronization has cancelled (state is now inactive)
	// but the source sync master was not notified.
	// This is only possible when the Force flags is set on the request.
	Aborted bool `json:"aborted,omitempty"`
	// Source is the endpoint of sync master on remote cluster that we used
	// to be synchronizing from.
	Source Endpoint `json:"source,omitempty"`
	// ClusterID is the ID of the local synchronization cluster.
	ClusterID string `json:"cluster_id,omitempty"`
	// Cancelling is set to true when synchronization is being cancelled.
	Cancelling bool `json:"cancelling,omitempty"`
	// Message is additional info which can be interpreted by a client.
	Message string `json:"message,omitempty"`
}

// MessageTimeoutInfo holds the timeout message info.
type MessageTimeoutInfo struct {
	MessageTimeout time.Duration `json:"messageTimeout"`
}

type EndpointsResponse struct {
	Endpoints Endpoint `json:"endpoints"`
}

type MastersResponse struct {
	Masters []MasterInfo `json:"masters"`
}

// SynchronizationBarrierStatus contains the status of the active synchronization barrier.
type SynchronizationBarrierStatus struct {
	// InSyncShards holds the number of shards that have reached the in-sync state.
	InSyncShards int `json:"in_sync_shards,omitempty"`
	// NotInSyncShards holds the number of shards that have not yet reached the in-sync state.
	NotInSyncShards int `json:"not_in_sync_shards,omitempty"`
	// SourceServerReadonly describes if the source DC is in read-only mode.
	SourceServerReadonly bool
}

// ShardChecksum contains a checksum for the shard.
type ShardChecksum struct {
	Checksum string `json:"checksum"`
}

// ShardsChecksum describes checksums for shards within one collection.
type ShardsChecksum struct {
	Shards map[int][]ShardChecksum `json:"shards"`
}

// CollectionsChecksum describes checksums for collections within one database
type CollectionsChecksum struct {
	Collections map[string]ShardsChecksum `json:"collections"`
}

// ChecksumSynchronizationResponse describes checksums for all databases.
type ChecksumSynchronizationResponse struct {
	Databases map[string]CollectionsChecksum `json:"databases"`
}

// ChecksumSynchronizationRequestOptions describes how checksums should be fetched.
type ChecksumSynchronizationRequestOptions struct {
	Timeout time.Duration `json:"timeout"`
}

// AddChecksums adds new checksum for the specific shard.
func (c *ChecksumSynchronizationResponse) AddChecksums(dbName, colName string, shardIndex int,
	checksum ...ShardChecksum) {

	if len(checksum) == 0 {
		return
	}

	if c.Databases == nil {
		c.Databases = make(map[string]CollectionsChecksum)
	}

	if _, ok := c.Databases[dbName]; !ok {
		collections := make(map[string]ShardsChecksum)

		c.Databases[dbName] = CollectionsChecksum{
			Collections: collections,
		}
	}

	if _, ok := c.Databases[dbName].Collections[colName]; !ok {
		shards := make(map[int][]ShardChecksum)

		c.Databases[dbName].Collections[colName] = ShardsChecksum{
			Shards: shards,
		}
	}

	shards := c.Databases[dbName].Collections[colName].Shards[shardIndex]
	shards = append(shards, checksum...)
	c.Databases[dbName].Collections[colName].Shards[shardIndex] = shards
}

// Merge merges checksums fot the same shard.
func (c *ChecksumSynchronizationResponse) Merge(from ChecksumSynchronizationResponse) {
	for dbname, database := range from.Databases {
		for colName, collections := range database.Collections {
			for shardIndex, checksums := range collections.Shards {
				c.AddChecksums(dbname, colName, shardIndex, checksums...)
			}
		}
	}
}

// GetChecksums gets checksums for the specific shard.
func (c *ChecksumSynchronizationResponse) GetChecksums(dbName, col string, shardIndex int) []ShardChecksum {
	if c.Databases == nil {
		return nil
	}

	database, ok := c.Databases[dbName]
	if !ok {
		return nil
	}

	if database.Collections == nil {
		return nil
	}

	collection, ok1 := database.Collections[col]
	if !ok1 {
		return nil
	}

	if _, ok := collection.Shards[shardIndex]; !ok {
		return nil
	}

	return collection.Shards[shardIndex]
}

// IsTheSame checks whether a shard contains the same checksums on each data center.
func (c *ChecksumSynchronizationResponse) IsTheSame(dbname, col string, shardIndex int) bool {
	checksums := c.GetChecksums(dbname, col, shardIndex)

	for i := range checksums {
		if checksums[i].Checksum != checksums[0].Checksum {
			// everything should equal to the first element
			return false
		}
	}

	return true
}

// DataCenterShardResponse stores information about a shard in the data center.
type DataCenterShardResponse struct {
	ID string `json:"ID"`
}

// DataCenterCollectionResponse stores information about a collection in the data center.
type DataCenterCollectionResponse struct {
	CollectionID string                          `json:"collection_id"`
	Shards       map[int]DataCenterShardResponse `json:"shards,omitempty"`
}

// DataCenterDatabaseResponse stores information about a database in the data center.
type DataCenterDatabaseResponse struct {
	DatabaseID          string                                  `json:"database_id"`
	Collections         map[string]DataCenterCollectionResponse `json:"collections,omitempty"`
	ExcludedCollections map[string]DataCenterCollectionResponse `json:"excluded_collections,omitempty"`
	Err                 string                                  `json:"error"`
}

// DataCenterResponse stores information about databases in the data center.
type DataCenterResponse struct {
	Databases map[string]DataCenterDatabaseResponse `json:"databases,omitempty"`
	ID        string                                `json:"id"`
}

func (dc *DataCenterResponse) GetExcludedShardsCount() int {
	count := 0
	for _, db := range dc.Databases {
		for _, col := range db.ExcludedCollections {
			count += len(col.Shards)
		}
	}
	return count
}

// DataCentersResponse stores information about databases in many data centers.
type DataCentersResponse struct {
	DataCenters []DataCenterResponse `json:"datacenters,omitempty"`
}

// ByID returns DataCenterResponse for DC with specified id
func (dcr *DataCentersResponse) ByID(id string) *DataCenterResponse {
	for _, r := range dcr.DataCenters {
		if r.ID == id {
			return &r
		}
	}
	return nil
}

// IsInactive checks whether the replication is turned off.
// It does not matter if the sync info comes from source, target or proxy data center.
func (s SyncInfo) IsInactive() bool {
	if len(s.Outgoing) > 0 {
		// There are some outgoing data, so definitely it is not inactive.
		return false
	}

	// From here on it is known that it is not source or proxy data center.
	if s.ShardsCount > 0 {
		// There are some incoming data, so definitely it is not inactive.
		return false
	}

	// The below status relates only to incoming data cluster.
	return s.Status.Normalize() == SyncStatusInactive
}

// GetOutgoingStatus returns status of a given cluster ID.
// Returns false if cluster ID is not found.
func (s SyncInfo) GetOutgoingStatus(id string) (SyncStatus, bool) {
	for _, info := range s.Outgoing {
		if info.ID == id {
			return info.Status.Normalize(), true
		}
	}

	return "", false
}
