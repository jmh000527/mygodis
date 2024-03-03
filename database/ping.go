package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// Ping PING
func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

func init() {
	RegisterCommand("PING", Ping, 1)
}
