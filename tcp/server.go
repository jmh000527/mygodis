package tcp

import (
	"context"
	"fmt"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Config 包含服务的配置信息
type Config struct {
	Address string // 服务监听的地址
}

// ListenAndServeWithSignal 启动TCP服务，并监听系统信号以优雅地关闭服务
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{}) // 用于通知关闭的通道
	sigChan := make(chan os.Signal)  // 用于接收系统信号的通道
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	// 启动协程监听系统信号
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	// 监听指定地址
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))

	// 调用ListenAndServe处理连接和关闭
	ListenAndServe(listener, handler, closeChan)

	return nil
}

// ListenAndServe 处理TCP连接和关闭服务的函数
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// 启动协程，监听关闭通道，收到信号时优雅地关闭服务
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		_ = listener.Close() // 关闭监听器
		_ = handler.Close()  // 关闭处理器
	}()

	// 在函数退出时确保关闭监听器和处理器
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("connection accepted")
		waitDone.Add(1)

		go func() {
			defer waitDone.Done()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait() // 等待所有连接处理完毕
}
