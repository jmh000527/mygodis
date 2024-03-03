package cluster

import "go-redis/interface/resp"

// execSelect 当前节点执行 SELECT 命令
func execSelect(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}
