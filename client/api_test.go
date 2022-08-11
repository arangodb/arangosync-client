//
// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksumSynchronizationResponse_AddChecksums(t *testing.T) {
	type args struct {
		DBName     string
		colName    string
		shardIndex int
		checksum   []ShardChecksum
	}
	tests := []struct {
		name string
		args []args
		want ChecksumSynchronizationResponse
	}{
		{
			name: "Empty list of checksums",
		},
		{
			name: "One shard added",
			args: []args{
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "abc",
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "abc",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Two shards with the same database, collection and index",
			args: []args{
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "abc",
						},
					},
				},
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "def",
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "abc",
										},
										{
											Checksum: "def",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Two shards with the same database, collection and different index",
			args: []args{
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "abc",
						},
					},
				},
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 1,
					checksum: []ShardChecksum{
						{
							Checksum: "def",
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "abc",
										},
									},
									1: {
										{
											Checksum: "def",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Two shards with the same database and different and different index",
			args: []args{
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "abc",
						},
					},
				},
				{
					DBName:     "_system",
					colName:    "_jobs",
					shardIndex: 1,
					checksum: []ShardChecksum{
						{
							Checksum: "def",
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "abc",
										},
									},
								},
							},
							"_jobs": {
								Shards: map[int][]ShardChecksum{
									1: {
										{
											Checksum: "def",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Two shards with different database and different and different index",
			args: []args{
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "abc",
						},
					},
				},
				{
					DBName:     "test",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "def",
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "abc",
										},
									},
								},
							},
						},
					},
					"test": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "def",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Many shards with many databases with many collections",
			args: []args{
				{
					DBName:     "_system",
					colName:    "_users",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "1",
						},
					},
				},
				{
					DBName:     "_system",
					colName:    "_jobs",
					shardIndex: 0,
					checksum: []ShardChecksum{
						{
							Checksum: "2",
						},
					},
				},
				{
					DBName:     "_system",
					colName:    "_jobs",
					shardIndex: 1,
					checksum: []ShardChecksum{
						{
							Checksum: "3",
						},
					},
				},
				{
					DBName:     "test",
					colName:    "test",
					shardIndex: 100,
					checksum: []ShardChecksum{
						{
							Checksum: "4",
						},
					},
				},
				{
					DBName:     "test",
					colName:    "test1",
					shardIndex: 2,
					checksum: []ShardChecksum{
						{
							Checksum: "5",
						},
					},
				},
				{
					DBName:     "test",
					colName:    "test2",
					shardIndex: 3,
					checksum: []ShardChecksum{
						{
							Checksum: "6",
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "1",
										},
									},
								},
							},
							"_jobs": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "2",
										},
									},
									1: {
										{
											Checksum: "3",
										},
									},
								},
							},
						},
					},
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									100: {
										{
											Checksum: "4",
										},
									},
								},
							},
							"test1": {
								Shards: map[int][]ShardChecksum{
									2: {
										{
											Checksum: "5",
										},
									},
								},
							},
							"test2": {
								Shards: map[int][]ShardChecksum{
									3: {
										{
											Checksum: "6",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ChecksumSynchronizationResponse{}

			for _, arg := range tt.args {
				c.AddChecksums(arg.DBName, arg.colName, arg.shardIndex, arg.checksum...)
			}

			assert.Equal(t, tt.want, c)
		})
	}
}

func TestChecksumSynchronizationResponse_GetChecksums(t *testing.T) {
	type fields struct {
		Databases map[string]CollectionsChecksum
	}
	type args struct {
		DBName     string
		col        string
		shardIndex int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []ShardChecksum
	}{
		{
			name: "Empty checksum object",
			args: args{
				DBName:     "test",
				col:        "test",
				shardIndex: 0,
			},
		},
		{
			name: "Database does not exist",
			fields: fields{
				Databases: map[string]CollectionsChecksum{},
			},
			args: args{
				DBName:     "test",
				col:        "test",
				shardIndex: 0,
			},
		},
		{
			name: "Collection object not initialized",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {},
				},
			},
			args: args{
				DBName:     "test",
				col:        "test",
				shardIndex: 0,
			},
		},
		{
			name: "Collection does not exits",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{},
					},
				},
			},
			args: args{
				DBName:     "test",
				col:        "test",
				shardIndex: 0,
			},
		},
		{
			name: "Shards object not initialized",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {},
						},
					},
				},
			},
			args: args{
				DBName:     "test",
				col:        "test",
				shardIndex: 0,
			},
		},
		{
			name: "Shard not found",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									1: {
										{
											Checksum: "abc",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				DBName:     "test",
				col:        "test",
				shardIndex: 0,
			},
		},
		{
			name: "One shard found with two checksums",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{
											Checksum: "checksum0-0",
										},
										{
											Checksum: "checksum0-1",
										},
									},
									1: {
										{
											Checksum: "checksum0",
										},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				DBName:     "test",
				col:        "test",
				shardIndex: 0,
			},
			want: []ShardChecksum{
				{
					Checksum: "checksum0-0",
				},
				{
					Checksum: "checksum0-1",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ChecksumSynchronizationResponse{
				Databases: tt.fields.Databases,
			}

			got := c.GetChecksums(tt.args.DBName, tt.args.col, tt.args.shardIndex)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestChecksumSynchronizationResponse_Merge(t *testing.T) {
	type fields struct {
		Databases map[string]CollectionsChecksum
	}
	type args struct {
		from ChecksumSynchronizationResponse
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   ChecksumSynchronizationResponse
	}{
		{
			name:   "Source object not initialized",
			fields: fields{},
			args:   args{},
			want:   ChecksumSynchronizationResponse{},
		},
		{
			name: "Source without databases",
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{},
				},
			},
		},
		{
			name: "Source collection not initialized",
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{
						"test": {},
					},
				},
			},
		},
		{
			name: "Source without collections",
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{
						"test": {
							Collections: map[string]ShardsChecksum{},
						},
					},
				},
			},
		},
		{
			name: "Source shards not initialized",
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{
						"test": {
							Collections: map[string]ShardsChecksum{
								"test": {},
							},
						},
					},
				},
			},
		},
		{
			name: "Source without shards",
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{
						"test": {
							Collections: map[string]ShardsChecksum{
								"test": {
									Shards: map[int][]ShardChecksum{},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merge two shards into uninitialized target",
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{
						"test": {
							Collections: map[string]ShardsChecksum{
								"test": {
									Shards: map[int][]ShardChecksum{
										0: {
											{Checksum: "1"},
										},
										1: {
											{Checksum: "2"},
										},
									},
								},
							},
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "1"},
									},
									1: {
										{Checksum: "2"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merge three shards within one collection",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"},
									},
									2: {
										{Checksum: "target2"},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{
						"test": {
							Collections: map[string]ShardsChecksum{
								"test": {
									Shards: map[int][]ShardChecksum{
										0: {
											{Checksum: "source1"},
										},
										1: {
											{Checksum: "source2"},
										},
									},
								},
							},
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"}, {Checksum: "source1"},
									},
									1: {
										{Checksum: "source2"},
									},
									2: {
										{Checksum: "target2"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Merge many shards",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test1": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"},
									},
									2: {
										{Checksum: "target2"},
									},
								},
							},
							"test2": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"},
									},
									2: {
										{Checksum: "target2"},
									},
								},
							},
						},
					},
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"},
									},
									2: {
										{Checksum: "target2"},
									},
								},
							},
						},
					},
					"only_source": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				from: ChecksumSynchronizationResponse{
					Databases: map[string]CollectionsChecksum{
						"test": {
							Collections: map[string]ShardsChecksum{
								"test1": {
									Shards: map[int][]ShardChecksum{
										0: {
											{Checksum: "source1"},
										},
									},
								},
								"test2": {
									Shards: map[int][]ShardChecksum{
										2: {
											{Checksum: "source1"},
										},
										1: {
											{Checksum: "source2"},
										},
									},
								},
							},
						},
						"_system": {
							Collections: map[string]ShardsChecksum{
								"_users": {
									Shards: map[int][]ShardChecksum{
										0: {
											{Checksum: "source1"},
										},
									},
								},
							},
						},
						"only_destination": {
							Collections: map[string]ShardsChecksum{
								"test": {
									Shards: map[int][]ShardChecksum{
										0: {
											{Checksum: "source1"},
										},
									},
								},
							},
						},
					},
				},
			},
			want: ChecksumSynchronizationResponse{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test1": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"}, {Checksum: "source1"},
									},
									2: {
										{Checksum: "target2"},
									},
								},
							},
							"test2": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"},
									},
									1: {
										{Checksum: "source2"},
									},
									2: {
										{Checksum: "target2"}, {Checksum: "source1"},
									},
								},
							},
						},
					},
					"_system": {
						Collections: map[string]ShardsChecksum{
							"_users": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"}, {Checksum: "source1"},
									},
									2: {
										{Checksum: "target2"},
									},
								},
							},
						},
					},
					"only_destination": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "source1"},
									},
								},
							},
						},
					},
					"only_source": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "target1"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ChecksumSynchronizationResponse{
				Databases: tt.fields.Databases,
			}

			c.Merge(tt.args.from)

			assert.Equal(t, tt.want, c)
		})
	}
}

func TestChecksumSynchronizationResponse_IsTheSame(t *testing.T) {
	type fields struct {
		Databases map[string]CollectionsChecksum
	}
	type args struct {
		dbname     string
		col        string
		shardIndex int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Response is the same",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "1"}, {Checksum: "1"},
									},
									1: {
										{Checksum: "1"}, {Checksum: "2"},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				dbname:     "test",
				col:        "test",
				shardIndex: 0,
			},
			want: true,
		},
		{
			name: "Response is not the same",
			fields: fields{
				Databases: map[string]CollectionsChecksum{
					"test": {
						Collections: map[string]ShardsChecksum{
							"test": {
								Shards: map[int][]ShardChecksum{
									0: {
										{Checksum: "1"}, {Checksum: "1"},
									},
									1: {
										{Checksum: "1"}, {Checksum: "2"},
									},
								},
							},
						},
					},
				},
			},
			args: args{
				dbname:     "test",
				col:        "test",
				shardIndex: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ChecksumSynchronizationResponse{
				Databases: tt.fields.Databases,
			}

			got := c.IsTheSame(tt.args.dbname, tt.args.col, tt.args.shardIndex)

			assert.Equal(t, tt.want, got)
		})
	}
}
