package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

// DEL
func execDEL(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("DEL", args...))
	}
	return reply.MakeIntReply(int64(deleted))
}

// EXISTS
func execEXISTS(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// KEYS
func execKEYS(db *DB, args [][]byte) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

// FLUSHDB
func execFLUSHDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine2("FLUSHDB", args...))
	return reply.MakeOkReply()
}

// TYPE
func execTYPE(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	}
	return &reply.UnknownErrReply{}
}

// RENAME
func execRENAME(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])
	entity, exists := db.GetEntity(src)
	if !exists {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	db.addAof(utils.ToCmdLine2("RENAME", args...))
	return reply.MakeOkReply()
}

// RENAMENX
func execRENAMENX(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	//检查key2是否存在
	_, exists := db.GetEntity(dest)
	if exists {
		return reply.MakeIntReply(0)
	}

	entity, exists := db.GetEntity(src)
	if !exists {
		reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	db.addAof(utils.ToCmdLine2("RENAMENX", args...))
	return reply.MakeIntReply(1)
}

func init() {
	RegisterCommand("DEL", execDEL, -2)
	RegisterCommand("EXISTS", execEXISTS, -2)
	RegisterCommand("KEYS", execKEYS, 2)
	RegisterCommand("FLUSHDB", execFLUSHDB, -1)
	RegisterCommand("TYPE", execTYPE, 2)
	RegisterCommand("RENAME", execRENAME, 3)
	RegisterCommand("RENAMENX", execRENAMENX, 3)
}
