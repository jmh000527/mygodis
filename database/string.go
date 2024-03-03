package database

import (
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// GET
func execGET(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes, _ := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}

// SET
func execSET(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine2("SET", args...))
	return reply.MakeOkReply()
}

// SETNX
func execSETNX(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine2("SETNX", args...))
	return reply.MakeIntReply(int64(result))
}

// GETSET
func execGETSET(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	db.PutEntity(key, &database.DataEntity{Data: value})
	db.addAof(utils.ToCmdLine2("GETSET", args...))
	return reply.MakeBulkReply(entity.Data.([]byte))
}

// STRLEN
func execSTRLEN(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("GET", execGET, 2)
	RegisterCommand("SET", execSET, -3)
	RegisterCommand("SETNX", execSETNX, 3)
	RegisterCommand("GETSET", execGETSET, 3)
	RegisterCommand("GSTRLEN", execSTRLEN, 2)
}
