package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// Rename 重命名一个键，源键和目标键必须在同一节点内
func Rename(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])
	// 选择源键和目标键所在的节点
	srcPeer := cluster.peerPicker.PickNode(src)
	destPeer := cluster.peerPicker.PickNode(dest)
	// 源键和目标键必须在同一节点内
	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	// 调用 relay 方法中继命令到源节点
	return cluster.relay(srcPeer, c, args)
}
