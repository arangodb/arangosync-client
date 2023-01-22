//
// Copyright 2017-2023 ArangoDB GmbH, Cologne, Germany
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
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/arangodb/go-driver"
)

// Get a prefix for names of channels that contain message
// going to this master.
func (c *client) ChannelPrefix(ctx context.Context) (string, error) {
	url := c.createURLs("/_api/channels/prefix", nil)

	var result ChannelPrefixInfo
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return "", maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return "", maskAny(err)
	}

	return result.Prefix, nil
}

// CreateMessageQueueConfig creates a remote MQ configuration.
func (c *client) CreateMessageQueueConfig(ctx context.Context) (MessageQueueConfigExt, error) {
	url := c.createURLs("/_api/mq/config", nil)

	var result MessageQueueConfigExt
	// It should be a POST method , but for the backward compatibility it is the GET method.
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return MessageQueueConfigExt{}, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return MessageQueueConfigExt{}, maskAny(err)
	}

	return result, nil
}

// UpdateMessageQueue sends requests to the remote DC to update an MQ config.
func (c *client) UpdateMessageQueue(ctx context.Context, input UpdateMQConfigRequest) error {
	url := c.createURLs("/_api/mq/config", nil)

	var result SyncInfo
	req, err := c.newRequests("PUT", url, input)
	if err != nil {
		return errors.WithMessage(err, "failed to create a request")
	}
	if err := c.do(ctx, req, &result); err != nil {
		return errors.WithMessage(err, "failed to perform a request")
	}

	return nil
}

// Status returns the current status of synchronization towards the local cluster.
func (c *client) Status(ctx context.Context, details GetSyncStatusDetails) (SyncInfo, error) {
	query := url.Values{"details": []string{string(details)}}
	url := c.createURLs("/_api/sync", query)

	var result SyncInfo
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return SyncInfo{}, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return SyncInfo{}, maskAny(err)
	}

	return result, nil
}

// Health performs a quick health check.
// Returns an error when anything is wrong. If so, check Status.
func (c *client) Health(ctx context.Context) error {
	url := c.createURLs("/_api/health", nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// GetEndpoints returns a list of all known master endpoints of this datacenter.
func (c *client) GetEndpoints(ctx context.Context) (Endpoint, error) {
	url := c.createURLs("/_api/endpoints", nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return nil, maskAny(err)
	}
	var result EndpointsResponse
	concurrent := true
	if err := c.do(ctx, req, &result, concurrent); err != nil {
		return nil, maskAny(err)
	}

	return result.Endpoints, nil
}

// GetLeaderEndpoint returns a list of master endpoints of the leader (syncmaster) of this datacenter.
// Length of returned list will 1 or the call will fail because no master is available.
func (c *client) GetLeaderEndpoint(ctx context.Context) (Endpoint, error) {
	url := c.createURLs("/_api/leader/endpoint", nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return nil, maskAny(err)
	}
	var result EndpointsResponse
	if err := c.do(ctx, req, &result); err != nil {
		return nil, maskAny(err)
	}

	return result.Endpoints, nil
}

// Return a list of known masters in this datacenter.
func (c *client) Masters(ctx context.Context) ([]MasterInfo, error) {
	url := c.createURLs("/_api/masters", nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return nil, maskAny(err)
	}
	var result MastersResponse
	if err := c.do(ctx, req, &result); err != nil {
		return nil, maskAny(err)
	}

	return result.Masters, nil
}

// Synchronize configures the master to synchronize the local cluster from a given remote cluster.
func (c *client) Synchronize(ctx context.Context, input SynchronizationRequest) error {
	url := c.createURLs("/_api/sync", nil)

	req, err := c.newRequests("POST", url, input)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// CancelSynchronization configures the master to stop & completely cancel the current synchronization of the
// local cluster from a remote cluster.
func (c *client) CancelSynchronization(ctx context.Context, input CancelSynchronizationRequest) (CancelSynchronizationResponse, error) {
	q := make(url.Values)

	url := c.createURLs("/_api/sync", q)
	var result CancelSynchronizationResponse
	req, err := c.newRequests("DELETE", url, input)
	if err != nil {
		return result, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return result, maskAny(err)
	}

	return result, nil
}

// CreateSynchronizationBarrier creates a barrier in the current synchronization
// that stops the source cluster from accepting any modifications
// such that the destination cluster can catch up.
func (c *client) CreateSynchronizationBarrier(ctx context.Context) error {
	url := c.createURLs("/_api/sync/barrier", nil)

	req, err := c.newRequests("POST", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// CancelSynchronizationBarrier removes the active barrier in the current synchronization
// that stops the source cluster from accepting any modifications
// such that the destination cluster can catch up.
// If there is not active barrier, this function returns without an error.
func (c *client) CancelSynchronizationBarrier(ctx context.Context) error {
	url := c.createURLs("/_api/sync/barrier", nil)

	req, err := c.newRequests("DELETE", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// GetSynchronizationBarrierStatus returns the status of the the active barrier
// in the current synchronization that stops the source cluster from accepting any modifications
// such that the destination cluster can catch up.
func (c *client) GetSynchronizationBarrierStatus(ctx context.Context) (SynchronizationBarrierStatus, error) {
	url := c.createURLs("/_api/sync/barrier", nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return SynchronizationBarrierStatus{}, maskAny(err)
	}
	var result SynchronizationBarrierStatus
	if err := c.do(ctx, req, &result); err != nil {
		return SynchronizationBarrierStatus{}, maskAny(err)
	}

	return result, nil
}

// Reset a failed shard synchronization.
func (c *client) ResetShardSynchronization(ctx context.Context, dbName, colName string, shardIndex int) error {
	url := c.createURLs(path.Join("/_api/sync/database", dbName, "collection", colName, "shard", strconv.Itoa(shardIndex), "reset"), nil)
	req, err := c.newRequests("PUT", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// GetMessageTimeout gets the maximum allowed time between messages in a task channel.
func (c *client) GetMessageTimeout(ctx context.Context) (MessageTimeoutInfo, error) {
	url := c.createURLs("/_api/message-timeout", nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return MessageTimeoutInfo{}, maskAny(err)
	}

	var messageTimeout MessageTimeoutInfo
	if err := c.do(ctx, req, &messageTimeout); err != nil {
		return MessageTimeoutInfo{}, maskAny(err)
	}

	return messageTimeout, nil
}

// Update the maximum allowed time between messages in a task channel.
func (c *client) SetMessageTimeout(ctx context.Context, timeout time.Duration) error {
	url := c.createURLs("/_api/message-timeout", nil)
	input := MessageTimeoutInfo{
		MessageTimeout: timeout,
	}
	req, err := c.newRequests("PUT", url, input)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// Create tasks to start synchronization of a shard in the given db+col.
func (c *client) SynchronizeShard(ctx context.Context, dbName, colName string, shardIndex int) error {
	url := c.createURLs(path.Join("/_api/sync/database", dbName, "collection", colName, "shard", strconv.Itoa(shardIndex)), nil)

	req, err := c.newRequests("POST", url, struct{}{})
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// CancelSynchronizeShard marks synchronization state to cancelling
// for the given shard (database name + collection name + shard index).
func (c *client) CancelSynchronizeShard(ctx context.Context, dbName, colName string, shardIndex int) error {
	url := c.createURLs(path.Join("/_api/sync/database", dbName, "collection", colName, "shard", strconv.Itoa(shardIndex)), nil)

	req, err := c.newRequests("DELETE", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// Report status of the synchronization of a shard back to the master.
func (c *client) SynchronizeShardStatus(ctx context.Context, entries []SynchronizationShardStatusRequestEntry) error {
	url := c.createURLs(path.Join("/_api/sync/multiple/status"), nil)

	input := SynchronizationShardStatusRequest{
		Entries: entries,
	}
	req, err := c.newRequests("PUT", url, input)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}
	return nil
}

// UpdateIncomingSynchronizationStatus updates details of synchronization status at master.
func (c *client) UpdateIncomingSynchronizationStatus(ctx context.Context, updateReq UpdateIncomingSynchronizationStatusRequest) error {
	url := c.createURLs(path.Join("/_api/sync/status"), nil)

	req, err := c.newRequests("PUT", url, updateReq)
	if err != nil {
		return errors.WithMessage(err, "failed to create a request")
	}
	if err := c.do(ctx, req, nil); err != nil {
		return errors.WithMessage(err, "failed to perform a request")
	}

	return nil
}

// IsChannelRelevant checks if a MQ channel is still relevant
func (c *client) IsChannelRelevant(ctx context.Context, channelName string) (bool, error) {
	url := c.createURLs(path.Join("/_api/mq/channel", url.PathEscape(channelName), "is-relevant"), nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return false, maskAny(err)
	}
	var result IsChannelRelevantResponse
	if err := c.do(ctx, req, &result); err != nil {
		return false, maskAny(err)
	}

	return result.IsRelevant, nil
}

// GetDirectMQTopicEndpoint returns an endpoint which can be used to fetch direct MQ messages from.
func (c *client) GetDirectMQTopicEndpoint(ctx context.Context, channelName, db, col string,
	shard uint) (DirectMQTopicEndpoint, error) {
	var query url.Values
	if len(db) > 0 && len(col) > 0 {
		// Persistent task does not have below values.
		query = make(url.Values)
		query["database"] = []string{db}
		query["collection"] = []string{col}
		query["shard"] = []string{strconv.Itoa(int(shard))}
	}

	url := c.createURLs(path.Join("/_api/mq/direct/channel", url.PathEscape(channelName), "endpoint"), query)
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return DirectMQTopicEndpoint{}, err
	}
	var result DirectMQTopicEndpoint
	if err := c.do(ctx, req, &result); err != nil {
		return DirectMQTopicEndpoint{}, err
	}

	return result, nil
}

// RenewDirectMQToken renews a given direct MQ token.
// This method requires a directMQ token for authentication.
func (c *client) RenewDirectMQToken(ctx context.Context, token string) (DirectMQToken, error) {
	url := c.createURLs("/_api/mq/direct/token/renew", nil)

	input := DirectMQTokenRequest{
		Token: token,
	}
	req, err := c.newRequests("POST", url, input)
	if err != nil {
		return DirectMQToken{}, maskAny(err)
	}
	var result DirectMQToken
	if err := c.do(ctx, req, &result); err != nil {
		return DirectMQToken{}, maskAny(err)
	}

	return result, nil
}

// CloneDirectMQToken creates a clone of a given direct MQ token.
// When the given token is revoked, the newly cloned token is also revoked.
// This method requires a directMQ token for authentication.
func (c *client) CloneDirectMQToken(ctx context.Context, token string) (DirectMQToken, error) {
	url := c.createURLs("/_api/mq/direct/token/clone", nil)

	input := DirectMQTokenRequest{
		Token: token,
	}
	req, err := c.newRequests("POST", url, input)
	if err != nil {
		return DirectMQToken{}, maskAny(err)
	}
	var result DirectMQToken
	if err := c.do(ctx, req, &result); err != nil {
		return DirectMQToken{}, maskAny(err)
	}

	return result, nil
}

// Start a task that sends inventory data to a receiving remote cluster.
func (c *client) OutgoingSynchronization(ctx context.Context, input OutgoingSynchronizationRequest) (OutgoingSynchronizationResponse, error) {
	url := c.createURLs("/_api/sync/outgoing", nil)

	req, err := c.newRequests("POST", url, input)
	if err != nil {
		return OutgoingSynchronizationResponse{}, maskAny(err)
	}
	var result OutgoingSynchronizationResponse
	if err := c.do(ctx, req, &result); err != nil {
		return OutgoingSynchronizationResponse{}, maskAny(err)
	}

	return result, nil
}

// Cancel sending synchronization data to the remote cluster with given ID.
func (c *client) CancelOutgoingSynchronization(ctx context.Context, remoteID string, serverMode driver.ServerMode) error {
	query := make(url.Values)
	query["server_mode"] = []string{string(serverMode)}

	url := c.createURLs(path.Join("/_api/sync/outgoing", remoteID), query)

	req, err := c.newRequests("DELETE", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// Create tasks to send synchronization data of a shard in the given db+col to a remote cluster.
func (c *client) OutgoingSynchronizeShard(ctx context.Context, remoteID, dbName, colName string, shardIndex int, input OutgoingSynchronizeShardRequest) error {
	url := c.createURLs(path.Join("/_api/sync/outgoing", remoteID, "database", dbName, "collection", colName, "shard", strconv.Itoa(shardIndex)), nil)

	req, err := c.newRequests("POST", url, input)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// Stop tasks to send synchronization data of a shard in the given db+col to a remote cluster.
func (c *client) CancelOutgoingSynchronizeShard(ctx context.Context, remoteID, dbName, colName string, shardIndex int) error {
	url := c.createURLs(path.Join("/_api/sync/outgoing", remoteID, "database", dbName, "collection", colName, "shard", strconv.Itoa(shardIndex)), nil)

	req, err := c.newRequests("DELETE", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// Report status of the synchronization of a shard back to the master.
func (c *client) OutgoingSynchronizeShardStatus(ctx context.Context, entries []SynchronizationShardStatusRequestEntry) error {
	url := c.createURLs(path.Join("/_api/sync/multiple/outgoing/status"), nil)

	input := SynchronizationShardStatusRequest{
		Entries: entries,
	}
	req, err := c.newRequests("PUT", url, input)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// OutgoingResetShardSynchronization sends request mark outgoing shard that it should be recreated.
func (c *client) OutgoingResetShardSynchronization(ctx context.Context, remoteID, dbName, colName string, shardIndex int,
	newControlChannel, newDataChannel string) error {
	url := c.createURLs(path.Join("/_api/sync/outgoing", remoteID, "database", dbName, "collection", colName, "shard",
		strconv.Itoa(shardIndex), "reset"), nil)

	input := OutgoingSynchronizeShardRequest{}
	input.Channels.Control = newControlChannel
	input.Channels.Data = newDataChannel
	req, err := c.newRequests("PUT", url, input)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// CreateOutgoingSynchronizationBarrier creates a barrier in the outgoing
// synchronization to the remote cluster with the given ID.
func (c *client) CreateOutgoingSynchronizationBarrier(ctx context.Context, remoteID string) error {
	url := c.createURLs(path.Join("/_api/sync/outgoing", remoteID, "barrier"), nil)

	req, err := c.newRequests("POST", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// CancelOutgoingSynchronizationBarrier removes the active barrier in the
// current synchronization to the remote cluster with given ID.
func (c *client) CancelOutgoingSynchronizationBarrier(ctx context.Context, remoteID string) error {
	url := c.createURLs(path.Join("/_api/sync/outgoing", remoteID, "barrier"), nil)

	req, err := c.newRequests("DELETE", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// Load configuration data from the master
func (c *client) ConfigureWorker(ctx context.Context, endpoint string) (WorkerConfiguration, error) {
	url := c.createURLs("/_api/worker/configure", nil)

	input := ConfigureWorkerRequest{
		Endpoint: endpoint,
	}
	req, err := c.newRequests("POST", url, input)
	if err != nil {
		return WorkerConfiguration{}, maskAny(err)
	}
	var result WorkerConfiguration
	if err := c.do(ctx, req, &result); err != nil {
		return WorkerConfiguration{}, maskAny(err)
	}

	return result, nil
}

// Return all registered workers
func (c *client) RegisteredWorkers(ctx context.Context) ([]WorkerRegistration, error) {
	url := c.createURLs("/_api/worker", nil)

	var result WorkerRegistrations
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return nil, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return nil, maskAny(err)
	}

	return result.Workers, nil
}

// Return info about a specific registered worker
func (c *client) RegisteredWorker(ctx context.Context, id string) (WorkerRegistration, error) {
	url := c.createURLs(path.Join("/_api/worker", id), nil)

	var result WorkerRegistration
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return WorkerRegistration{}, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return WorkerRegistration{}, maskAny(err)
	}

	return result, nil
}

// RegisterWorker registers or updates the new worker in the ArangoSync master leader.
func (c *client) RegisterWorker(ctx context.Context, endpoint, token, hostID, dbServerAffinity string) (WorkerRegistrationResponse, error) {
	url := c.createURLs("/_api/worker", nil)

	input := WorkerRegistrationRequest{
		Endpoint:         endpoint,
		Token:            token,
		HostID:           hostID,
		DBServerAffinity: dbServerAffinity,
	}
	var result WorkerRegistrationResponse
	req, err := c.newRequests("PUT", url, input)
	if err != nil {
		return WorkerRegistrationResponse{}, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return WorkerRegistrationResponse{}, maskAny(err)
	}

	return result, nil
}

// Remove the registration of a worker
func (c *client) UnregisterWorker(ctx context.Context, id string) error {
	url := c.createURLs(path.Join("/_api/worker", id), nil)

	req, err := c.newRequests("DELETE", url, nil)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// Get info about a specific task
func (c *client) Task(ctx context.Context, id string) (TaskInfo, error) {
	url := c.createURLs(path.Join("/_api/task/id", id), nil)

	var result TaskInfo
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return TaskInfo{}, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return TaskInfo{}, maskAny(err)
	}

	return result, nil
}

// Get all known tasks
func (c *client) Tasks(ctx context.Context) ([]TaskInfo, error) {
	url := c.createURLs("/_api/task", nil)

	var result TasksResponse
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return nil, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return nil, maskAny(err)
	}

	return result.Tasks, nil
}

// Get all known tasks for a given channel
func (c *client) TasksByChannel(ctx context.Context, channelName string) ([]TaskInfo, error) {
	url := c.createURLs(path.Join("/_api/task/channel", channelName), nil)

	var result TasksResponse
	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return nil, maskAny(err)
	}
	if err := c.do(ctx, req, &result); err != nil {
		return nil, maskAny(err)
	}

	return result.Tasks, nil
}

// Notify the master that a task with given ID has completed.
func (c *client) TaskCompleted(ctx context.Context, taskID string, info TaskCompletedRequest) error {
	url := c.createURLs(path.Join("/_api/task", taskID, "completed"), nil)

	req, err := c.newRequests("PUT", url, info)
	if err != nil {
		return maskAny(err)
	}
	if err := c.do(ctx, req, nil); err != nil {
		return maskAny(err)
	}

	return nil
}

// GetChecksumShardSynchronization gets checksum of the data for specific shard.
func (c *client) GetChecksumShardSynchronization(ctx context.Context, dbName, colName string,
	shardIndex int, options ChecksumSynchronizationRequestOptions) (ChecksumSynchronizationResponse, error) {

	url := c.createURLs(path.Join("/_api/sync/database", dbName, "collection", colName, "shard",
		strconv.Itoa(shardIndex), "checksum"), nil)

	req, err := c.newRequests("GET", url, options)
	if err != nil {
		return ChecksumSynchronizationResponse{}, maskAny(err)
	}

	result := ChecksumSynchronizationResponse{}
	if err = c.do(ctx, req, &result); err != nil {
		return ChecksumSynchronizationResponse{}, maskAny(err)
	}

	return result, nil
}

// GetDataCentersInfo gets shards for all datacenters which are included in synchronization.
func (c *client) GetDataCentersInfo(ctx context.Context) (DataCentersResponse, error) {

	url := c.createURLs(path.Join("/_api/sync/shards"), nil)

	req, err := c.newRequests("GET", url, nil)
	if err != nil {
		return DataCentersResponse{}, maskAny(err)
	}

	result := DataCentersResponse{}
	if err := c.do(ctx, req, &result); err != nil {
		return DataCentersResponse{}, maskAny(err)
	}

	return result, nil
}
