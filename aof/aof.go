package aof

import (
	"go-redis/config"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
	"sync"
)

// CmdLine 是 [][]byte 的别名，表示一条命令行
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// AofHandler 接收来自通道的消息并将其写入AOF文件
type AofHandler struct {
	database    databaseface.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	// aof协程完成AOF任务并准备关闭时，通过此通道向主协程发送消息
	aofFinished chan struct{}
	// 为开始/结束AOF重写进程暂停AOF
	pausingAof sync.RWMutex
	currentDB  int
}

// LoadAof 从AOF文件中加载命令并执行。
// 参数 maxBytes 用于限制读取的最大字节数，若为0则不限制。
// 该函数会首先关闭AOF通道以防止继续写入，并在函数结束时重新打开AOF通道。
func (handler *AofHandler) LoadAof(maxBytes int) {
	// 删除aofChan以防止再次写入
	aofChan := handler.aofChan
	handler.aofChan = nil
	defer func(aofChan chan *payload) {
		handler.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	// 使用resp/parser包中的ParseStream函数解析AOF文件内容
	ch := parser.ParseStream(reader)
	// 创建一个FakeConn用于执行解析出的命令
	fakeConn := &connection.FakeConn{} // 仅用于保存dbIndex

	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		// 使用AOF处理器的数据库接口执行解析出的命令
		ret := handler.database.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}

// handleAof 监听 AOF 通道，将命令写入 AOF 文件。
// 该函数负责将AOF通道中的命令逐一写入AOF文件，实现AOF的持久化。
// 在写入命令之前，会检查是否需要切换到新的数据库，如果需要，则先写入SELECT命令。
// 函数使用 RLock/RUnlock 保证在写入文件期间不会被其他协程暂停AOF。
func (handler *AofHandler) handleAof() {
	// 序列化执行
	handler.currentDB = 0
	// 循环监听AOF通道，处理传入的命令
	for p := range handler.aofChan {
		// 防止其他协程暂停AOF
		handler.pausingAof.RLock()
		// 检查是否需要切换到新的数据库
		if p.dbIndex != handler.currentDB {
			// 选择数据库，构建SELECT命令并写入AOF文件
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue // 跳过此命令
			}
			handler.currentDB = p.dbIndex
		}

		// 将命令转换为字节并写入AOF文件
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
		// 解锁，允许其他协程暂停AOF
		handler.pausingAof.RUnlock()
	}
	// AOF通道关闭，发送完成信号
	handler.aofFinished <- struct{}{}
}

// AddAof 通过通道将命令发送到aof协程
func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// Close 优雅地停止AOF持久化过程
func (handler *AofHandler) Close() {
	if handler.aofFile != nil {
		close(handler.aofChan)
		<-handler.aofFinished // 等待AOF完成
		err := handler.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}

// NewAofHandler 创建一个新的AofHandler
func NewAofHandler(database databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.database = database
	handler.LoadAof(0)
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}
