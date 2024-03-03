package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// Del 从集群中原子性地移除给定的键，这些键可以分布在任何节点上
// 如果给定的键分布在不同的节点上，Del 将使用 try-commit-catch 来移除它们
func Del(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 广播命令到集群中的所有节点
	replies := cluster.broadcast(c, args)
	// 遍历所有节点的回复
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
		intReply, ok := v.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}
	// 如果没有错误回复，返回包含删除数量的整数回复
	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	// 如果有错误回复，返回包含错误信息的错误回复
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
