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
	"sync"
	"time"
)

type ShardSyncStatus struct {
	InSync  bool
	Message string        // Reason of why the shard is in-sync or not in-sync
	Delay   time.Duration // The time it took to fetch shard sync status
}

type CollectionSyncStatus struct {
	ID     string                  // internal collection ID
	Shards map[int]ShardSyncStatus // shard index -> shard status
	Error  string
}

type DatabaseSyncStatus struct {
	ID          string                          // internal DB ID
	Collections map[string]CollectionSyncStatus // collection name -> collection status
	Error       string
}

type DCSyncStatus struct {
	Databases map[string]DatabaseSyncStatus // DB name -> DB status
	mutex     sync.Mutex
}

func (s *DCSyncStatus) AllInSync() bool {
	for _, db := range s.Databases {
		if db.Error != "" {
			return false
		}
		for _, col := range db.Collections {
			if col.Error != "" {
				return false
			}
			for _, s := range col.Shards {
				if !s.InSync {
					return false
				}
			}
		}
	}
	return true
}

func (s *DCSyncStatus) setShardStatus(dbName, dbID, colName, colID string, shardIndex int, inSync bool, delay time.Duration, msg string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	status := ShardSyncStatus{
		InSync:  inSync,
		Message: msg,
		Delay:   delay,
	}
	s.ensureCollection(dbName, dbID, colName, colID)
	s.Databases[dbName].Collections[colName].Shards[shardIndex] = status
}

func (s *DCSyncStatus) setDBError(dbName, dbID, errStr string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.ensureDB(dbName, dbID)
	db := s.Databases[dbName]
	db.Error = errStr
}

func (s *DCSyncStatus) ensureDB(dbName, dbID string) {
	if s.Databases == nil {
		s.Databases = make(map[string]DatabaseSyncStatus)
	}
	if _, ok := s.Databases[dbName]; !ok {
		s.Databases[dbName] = DatabaseSyncStatus{
			ID:          dbID,
			Collections: make(map[string]CollectionSyncStatus),
		}
	}
}

func (s *DCSyncStatus) setCollectionError(dbName, dbID, colName, colID, errStr string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.ensureCollection(dbName, dbID, colName, colID)
	col := s.Databases[dbName].Collections[colName]
	col.Error = errStr
}

func (s *DCSyncStatus) ensureCollection(dbName, dbID, colName, colID string) {
	s.ensureDB(dbName, dbID)
	db := s.Databases[dbName]
	if _, ok := db.Collections[colName]; !ok {
		db.Collections[colName] = CollectionSyncStatus{
			ID:     colID,
			Shards: map[int]ShardSyncStatus{},
		}
	}
}
