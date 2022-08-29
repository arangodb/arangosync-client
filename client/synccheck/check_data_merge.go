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
	"fmt"

	"github.com/arangodb/arangosync-client/client"
)

type shardInfo struct {
	Database     string
	DatabaseID   string
	CollectionID string
	Collection   string
	ShardIndex   int
}

type checkShardMerge struct {
	shardIDs []string
}

type checkCollectionMerge struct {
	collectionID string
	shards       map[int]checkShardMerge
	err          string
}

type checkDatabaseMerge struct {
	databaseID  string
	collections map[string]checkCollectionMerge
	err         string
}

type checkDataMerge struct {
	databases    map[string]checkDatabaseMerge
	dataCenterID string
}

func (c *checkDataMerge) Add(newDataCenter client.DataCenterResponse) {
	if c.databases == nil {
		c.create(newDataCenter)
		return
	}

	c.merge(newDataCenter)
}

// getCheckDataInfo returns according to the filter a slice of missing shards which should be synchronized
// and the number of shards which should be checked.
func (c *checkDataMerge) getCheckDataInfo(filter ShardFilter, shards []client.ShardSyncInfo) (int, []shardInfo) {
	var counter int

	var missingShards []shardInfo
	for databaseName, database := range c.databases {
		if !filter.IncludeDatabase(databaseName) {
			continue
		}

		if len(database.err) > 0 {
			continue
		}

		for collectionName, collection := range database.collections {
			if !filter.IncludeCollection(collectionName) {
				continue
			}

			if len(collection.err) > 0 {
				continue
			}

			for shardIndex := range collection.shards {
				if !filter.IncludeShardIndex(shardIndex) {
					continue
				}
				counter++

				found := false
				for _, s := range shards {
					if s.Database == databaseName && s.Collection == collectionName && s.ShardIndex == shardIndex {
						found = true
						break
					}
				}

				if !found {
					missingShards = append(missingShards, shardInfo{
						Database:     databaseName,
						DatabaseID:   database.databaseID,
						Collection:   collectionName,
						CollectionID: collection.collectionID,
						ShardIndex:   shardIndex,
					})
				}
			}
		}
	}

	return counter, missingShards
}

func (c *checkDataMerge) mergeShards(databaseName string, newDataCenterID string, newDatabase client.DataCenterDatabaseResponse) {
	database, ok := c.databases[databaseName]
	if !ok {
		return
	}

	for collectionName, col := range newDatabase.Collections {
		if _, ok := database.collections[collectionName]; !ok {
			database.collections[collectionName] = checkCollectionMerge{
				collectionID: col.CollectionID,
				err: fmt.Sprintf("collection %s in database %s does not exist in the %s data center",
					collectionName, databaseName, c.dataCenterID),
			}
		}
	}

	for collectionName, collection := range database.collections {
		if len(collection.err) > 0 {
			continue
		}

		newCollection, ok := newDatabase.Collections[collectionName]
		if !ok { // nolint: gocritic
			collection.err = fmt.Sprintf("collection %s in database %s does not exist in the %s data center",
				collectionName, databaseName, newDataCenterID)
		} else if len(newCollection.Shards) != len(collection.shards) {
			collection.err = fmt.Sprintf("collection %s has different number of shards", collectionName)
		} else {
			for i, newShard := range newCollection.Shards {
				if shard, ok := collection.shards[i]; ok {
					shard.shardIDs = append(shard.shardIDs, newShard.ID)
					collection.shards[i] = shard
				}
			}
		}

		database.collections[collectionName] = collection
	}
}

func (c *checkDataMerge) merge(newDataCenter client.DataCenterResponse) {
	for databaseName, newDatabase := range newDataCenter.Databases {
		database, ok := c.databases[databaseName]
		if !ok {
			c.databases[databaseName] = checkDatabaseMerge{
				databaseID: newDatabase.DatabaseID,
				err:        fmt.Sprintf("database %s does not exist in the %s data center", databaseName, c.dataCenterID),
			}
			continue
		}

		if len(database.err) > 0 {
			continue
		}

		if len(newDatabase.Err) > 0 {
			database.err = newDatabase.Err
			c.databases[databaseName] = database
			continue
		}

		c.mergeShards(databaseName, newDataCenter.ID, newDatabase)
	}

	for databaseName, database := range c.databases {
		if len(database.err) > 0 {
			continue
		}

		newDatabase, ok := newDataCenter.Databases[databaseName]
		if !ok {
			database.err = fmt.Sprintf("database %s does not exist in the %s data center", databaseName,
				newDataCenter.ID)
			c.databases[databaseName] = database
			continue
		}

		for collectionName, collection := range database.collections {
			if len(collection.err) > 0 {
				continue
			}

			if _, ok := newDatabase.Collections[collectionName]; ok {
				continue
			}

			collection.err = fmt.Sprintf("collection %s in database %s does not exist in the %s data center",
				collectionName, databaseName, newDataCenter.ID)
			database.collections[collectionName] = collection
		}
	}
}

func (c *checkDataMerge) create(newDataCenter client.DataCenterResponse) {
	databases := make(map[string]checkDatabaseMerge)
	for databaseName, newDatabase := range newDataCenter.Databases {
		if len(newDatabase.Err) > 0 {
			databases[databaseName] = checkDatabaseMerge{
				databaseID: newDatabase.DatabaseID,
				err:        newDatabase.Err,
			}
			continue
		}

		collections := make(map[string]checkCollectionMerge)
		for collectionName, collection := range newDatabase.Collections {
			shards := make(map[int]checkShardMerge)
			for shardIndex, shard := range collection.Shards {
				shards[shardIndex] = checkShardMerge{
					shardIDs: []string{shard.ID},
				}
			}

			collections[collectionName] = checkCollectionMerge{
				collectionID: collection.CollectionID,
				shards:       shards,
			}
		}

		if len(collections) > 0 {
			databases[databaseName] = checkDatabaseMerge{
				databaseID:  newDatabase.DatabaseID,
				collections: collections,
			}
		}

	}

	if len(databases) > 0 {
		c.databases = databases
		c.dataCenterID = newDataCenter.ID
	}
}
