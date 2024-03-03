package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// FlushDB 移除当前集群数据库中的所有数据
func FlushDB(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 广播命令到集群中的所有节点
	replies := cluster.broadcast(c, args)
	// 检查所有节点的回复，如果有错误回复则返回错误
	var errReply reply.ErrorReply
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
	}
	// 如果没有错误回复，返回成功回复
	if errReply == nil {
		return &reply.OkReply{}
	}
	// 如果有错误回复，返回包含错误信息的错误回复
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
