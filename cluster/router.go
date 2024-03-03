package cluster

import "go-redis/interface/resp"

// CmdLine 是 [][]byte 的别名，表示一个命令行
type CmdLine = [][]byte

// CmdFunc 表示一个 Redis 命令的处理器
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["ping"] = ping

	routerMap["del"] = Del

	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["rename"] = Rename
	routerMap["renamenx"] = Rename

	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc

	routerMap["flushdb"] = FlushDB
	routerMap["select"] = execSelect

	return routerMap
}

// 将命令转发到负责的节点，并将其回复返回给客户端
func defaultFunc(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	key := string(args[1])
	// 通过key使用一致性哈希寻找节点，并转发
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, args)
}
