package cluster

import (
	"context"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/client"
	"go-redis/resp/reply"
	"strconv"
)

// getPeerClient 获取与指定节点建立的客户端连接
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	//找到与对应节点的连接池
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection pool not found")
	}
	//从找到的连接池中取出一个连接
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

// returnPeerClient 将客户端连接返还到连接池
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection pool not found")
	}
	return pool.ReturnObject(context.Background(), peerClient)
}

// relay 将命令中继到指定的节点
// 通过 c.GetDBIndex() 选择数据库
// 不能调用 self 节点的 Prepare、Commit、execRollback
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self {
		// 到自身数据库执行
		return cluster.db.Exec(c, args)
	}
	//获取一个到目标节点的连接
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	//中继完成后归还连接到连接池
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	//发送命令到指定节点，并选择对于db
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// broadcast 将命令广播到集群中的所有节点
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	result := make(map[string]resp.Reply)
	//向所有节点进行中继，并获取回复
	for _, node := range cluster.nodes {
		reply := cluster.relay(node, c, args)
		result[node] = reply
	}
	return result
}
