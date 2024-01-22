package cluster

import (
	"context"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ConnectOptions struct {
	Address     string
	Username    string
	Password    string
	ClusterName string
}

func Connect(ctx context.Context, opts ConnectOptions) (*Cluster, error) {
	conn, err := openConnection(ctx, opts.Address, opts.Username, opts.Password)
	if err != nil {
		return nil, err
	}

	if opts.ClusterName == "" {
		return NewCluster([]Shard{
			NewShard(opts.Address, conn),
		}), nil
	}

	shards, err := openClusterShards(ctx, conn, opts.Username, opts.Password, opts.ClusterName)

	return NewCluster(shards), err
}

func openClusterShards(ctx context.Context, conn clickhouse.Conn, userName, password, clusterName string) ([]Shard, error) {
	shardsAddresses, err := getShardsAddresses(ctx, conn, clusterName)
	if err != nil {
		return nil, err
	}

	shards := make([]Shard, 0, len(shardsAddresses))

	for _, shardAddress := range shardsAddresses {
		conn, err = openConnection(ctx, shardAddress, userName, password)
		if err != nil {
			return nil, err
		}

		shards = append(shards, NewShard(shardAddress, conn))
	}

	return shards, nil
}

func openConnection(ctx context.Context, address, userName, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Username: userName,
			Password: password,
		},
	})
	if err != nil {
		return nil, err
	}

	return conn, conn.Ping(ctx)
}

func getShardsAddresses(ctx context.Context, conn clickhouse.Conn, clusterName string) ([]string, error) {
	// TODO: handle replicas
	rows, err := conn.Query(ctx, `
		SELECT 
			host_address, port
		FROM 
		    system.clusters
		WHERE 
		    cluster = $1
	`, clusterName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var result []string

	for rows.Next() {
		var host string
		var port int

		result = append(result, host+":"+strconv.Itoa(port))
	}

	return result, nil
}
