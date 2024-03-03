package handler

import (
	"context"
	"errors"
	"go-redis/cluster"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

// RespHandler 定义了处理 Redis 协议请求的结构体
type RespHandler struct {
	activeConn sync.Map              // 保存活跃的连接
	db         databaseface.Database // 数据库接口
	closing    atomic.Boolean        // 用于标记关闭状态
}

// MakeHandler 创建 RespHandler 实例的工厂函数
func MakeHandler() *RespHandler {
	var db databaseface.Database
	// 创建数据库实例，集群或单体
	if config.Properties.Self != "" &&
		len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewStandaloneDatabase()
	}

	return &RespHandler{
		db: db,
	}
}

// closeClient 用于关闭客户端连接
func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	// 在数据库中处理客户端关闭事件
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
}

// Handle 处理客户端连接的函数
func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})

	// 解析客户端发来的命令
	ch := parser.ParseStream(conn)
	for payload := range ch {
		// 解析错误处理
		if payload.Err != nil {
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			// 协议错误，向客户端回复错误信息
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}

		// 解析成功，执行命令
		if payload.Data == nil {
			continue
		}
		multiBulkReply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multiBulkReply")
			continue
		}

		// 在数据库中执行命令，并将结果写回客户端
		result := r.db.Exec(client, multiBulkReply.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			// 未知错误处理，向客户端回复错误信息
			unknownErrReply := reply.UnknownErrReply{}
			_ = client.Write(unknownErrReply.ToBytes())
		}
	}
}

// Close 关闭 RespHandler，释放资源
func (r *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	r.closing.Set(true)

	// 遍历所有活跃的连接，关闭它们
	r.activeConn.Range(func(key, value any) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})

	// 关闭数据库
	r.db.Close()

	return nil
}
