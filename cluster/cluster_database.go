package cluster

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strings"
)

// ClusterDatabase 表示 godis 集群中的一个节点
// 它持有部分数据并协调其他节点完成事务
type ClusterDatabase struct {
	self           string                      //节点自己的名称
	nodes          []string                    //整个集群的节点切片，包含当前节点
	peerPicker     *consistenthash.NodeMap     //一致性哈希的管理器
	peerConnection map[string]*pool.ObjectPool //对其他各个节点的连接池映射
	db             databaseface.Database       //对应的单体数据库（standalone_database）
}

// MakeClusterDatabase 创建并启动集群中的一个节点
func MakeClusterDatabase() *ClusterDatabase {
	//新建待返回的ClusterDatabase结构体
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	//将所有节点包括当前节点添加到nodes切片中
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	//将所有节点，nodes切片，添加到一致性哈希管理器
	cluster.peerPicker.AddNode(nodes...)
	//初始化对每一个兄弟节点的连接池
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	//写入ClusterDatabase结构体
	cluster.nodes = nodes
	return cluster
}

// Close 停止当前集群节点
func (cluster *ClusterDatabase) Close() {
	// 调用底层数据库的 Close 方法停止当前集群节点
	cluster.db.Close()
}

var router = makeRouter()

// Exec 在集群上执行命令
func (cluster *ClusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	// 使用 defer 捕获可能的 panic，并返回 UnknownErrReply
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()
	// 获取命令名称并转为小写
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 查找命令处理函数
	cmdFunc, ok := router[cmdName]
	if !ok {
		// 如果命令不被支持，返回错误回复
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	// 调用命令处理函数，并返回结果
	result = cmdFunc(cluster, c, cmdLine)
	return result
}

// AfterClientClose 在客户端关闭连接后执行一些清理工作
func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	// 调用底层数据库的 AfterClientClose 方法执行清理工作
	cluster.db.AfterClientClose(c)
}
