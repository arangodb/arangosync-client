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

package synccheck

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arangodb/arangosync-client/client"
)

type ShardFilter interface {
	// IncludeDatabase checks whether a database should be included according to the filter.
	IncludeDatabase(databaseName string) bool
	// IncludeCollection checks whether a collection should be included according to the filter.
	IncludeCollection(colName string) bool
	// IncludeShardIndex checks whether a shard index should be included according to the filter.
	IncludeShardIndex(shardIndex int) bool
}

type NilShardsFilter struct {
}

func (NilShardsFilter) IncludeDatabase(_ string) bool {
	return true
}
func (NilShardsFilter) IncludeCollection(_ string) bool {
	return true
}
func (NilShardsFilter) IncludeShardIndex(_ int) bool {
	return true
}

// IncludeAllShardsFilter will not filter out any shards
var IncludeAllShardsFilter = NilShardsFilter{}

type ProgressCallback func(fetchedShards, totalShards int)

// SynchronizationChecker allows fetching shards synchronization status
type SynchronizationChecker struct {
	client                client.API
	shardFetchTimeout     time.Duration
	shardFilter           ShardFilter
	progressCallback      ProgressCallback
	maxConcurrentRequests int
}

func NewSynchronizationChecker(c client.API, shardFetchTimeout time.Duration) *SynchronizationChecker {
	return &SynchronizationChecker{
		client:                c,
		shardFilter:           IncludeAllShardsFilter,
		shardFetchTimeout:     shardFetchTimeout,
		maxConcurrentRequests: 30, // default value, can be updated via setter
		progressCallback:      func(_, _ int) {},
	}
}

func (s *SynchronizationChecker) SetProgressCallback(cb ProgressCallback) {
	s.progressCallback = cb
}

func (s *SynchronizationChecker) SetMaxConcurrentRequests(max int) {
	s.maxConcurrentRequests = max
}

func (s *SynchronizationChecker) SetShardFilter(f ShardFilter) {
	s.shardFilter = f
}

func (s *SynchronizationChecker) CheckSync(ctx context.Context, dataCenters *client.DataCentersResponse, localShards []client.ShardSyncInfo) (*DCSyncStatus, error) {
	mergedDataCenters := checkDataMerge{}
	for _, dataCenter := range dataCenters.DataCenters {
		mergedDataCenters.Add(dataCenter)
	}

	numShardsTotal, missingShards := mergedDataCenters.getCheckDataInfo(s.shardFilter, localShards)
	numShardsLeftToFetch := int32(numShardsTotal)
	s.progressCallback(0, numShardsTotal)

	result := &DCSyncStatus{}
	for _, shard := range missingShards {
		result.setShardStatus(shard.Database, shard.DatabaseID, shard.Collection, shard.CollectionID, shard.ShardIndex, false, 0, "Shard is not turned on for synchronizing")
	}

	concurrentLimit := make(chan bool, s.maxConcurrentRequests)

	var wg sync.WaitGroup
	for dbName, database := range mergedDataCenters.databases {
		if !s.shardFilter.IncludeDatabase(dbName) {
			continue
		}

		if len(database.err) > 0 {
			result.setDBError(dbName, database.databaseID, database.err)
			continue
		}

		for collectionName, collection := range database.collections {
			if !s.shardFilter.IncludeCollection(collectionName) {
				continue
			}

			if len(collection.err) > 0 {
				result.setCollectionError(dbName, database.databaseID, collectionName, collection.collectionID, collection.err)
				continue
			}

			for shardIndex := range collection.shards {
				if !s.shardFilter.IncludeShardIndex(shardIndex) {
					continue
				}

				skipShard := false
				for _, shard := range missingShards {
					if collectionName == shard.Collection && shardIndex == shard.ShardIndex && dbName == shard.Database {
						skipShard = true
						break
					}
				}
				if skipShard {
					continue
				}

				concurrentLimit <- true
				if err := ctx.Err(); err != nil {
					return nil, err
				}

				wg.Add(1)
				go func(dbName, dbID, collectionName, collectionID string, shardIndex int) {
					defer func() {
						<-concurrentLimit
						atomic.AddInt32(&numShardsLeftToFetch, -1)
						numFetchedShards := numShardsTotal - int(atomic.LoadInt32(&numShardsLeftToFetch))
						s.progressCallback(numFetchedShards, numShardsTotal)
						wg.Done()
					}()

					inSync, delay, msg := s.processShard(ctx, dbName, collectionName, shardIndex)
					result.setShardStatus(dbName, dbID, collectionName, collectionID, shardIndex, inSync, delay, msg)
				}(dbName, database.databaseID, collectionName, collection.collectionID, shardIndex)
			}
		}
	}

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *SynchronizationChecker) processShard(ctx context.Context, dbName, colName string, shardIndex int) (bool, time.Duration, string) {
	fetchShardCtx, cancel := context.WithTimeout(ctx, s.shardFetchTimeout)
	defer cancel()

	timeStart := time.Now()
	options := client.ChecksumSynchronizationRequestOptions{
		Timeout: s.shardFetchTimeout,
	}
	resp, err := s.client.Master().GetChecksumShardSynchronization(fetchShardCtx, dbName, colName, shardIndex, options)
	if err != nil {
		return false, 0, err.Error()
	}
	delay := time.Since(timeStart)

	checksums := resp.GetChecksums(dbName, colName, shardIndex)
	isInSync := resp.IsTheSame(dbName, colName, shardIndex)

	return isInSync, delay, fmt.Sprintf("%v", checksums)
}
