package database

import (
	"fmt"
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

// StandaloneDatabase 是一个包含多个数据库集合的单机 Redis 数据库
type StandaloneDatabase struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler // 处理 AOF 持久化
}

// NewStandaloneDatabase 创建一个 Redis 数据库
func NewStandaloneDatabase() *StandaloneDatabase {
	mdb := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := MakeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	// 判断是否启用 AOF
	if config.Properties.AppendOnly {
		// 创建 AOF 处理器
		aofHandler, err := aof.NewAofHandler(mdb)
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			// 避免闭包问题，将 AOF 操作绑定到每个数据库实例
			singleDB := db
			singleDB.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}
	}
	return mdb
}

// Exec 执行命令
// 参数 `cmdLine` 包含命令及其参数，例如："set key value"
func (mdb *StandaloneDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			// 处理 select 命令参数错误
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// 普通命令
	dbIndex := c.GetDBIndex()
	if dbIndex >= len(mdb.dbSet) {
		// 处理数据库索引超出范围的错误
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// Close 优雅关闭数据库
func (mdb *StandaloneDatabase) Close() {
	// 在这里执行数据库关闭操作（如果有的话）
}

// AfterClientClose 在客户端关闭连接后执行一些清理工作
func (mdb *StandaloneDatabase) AfterClientClose(c resp.Connection) {
	// 在这里执行客户端关闭连接后的清理工作
}

// execSelect 处理 select 命令
func execSelect(c resp.Connection, mdb *StandaloneDatabase, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		// 处理无效的数据库索引错误
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		// 处理数据库索引超出范围的错误
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
