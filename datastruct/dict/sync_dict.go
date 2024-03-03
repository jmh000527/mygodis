package dict

import "sync"

type SyncDict struct {
	m sync.Map
}

func (sd *SyncDict) Get(key string) (val interface{}, exists bool) {
	value, ok := sd.m.Load(key)
	return value, ok
}

func (sd *SyncDict) Len() int {
	length := 0
	sd.m.Range(func(key, value any) bool {
		length++
		return true
	})
	return length
}

func (sd *SyncDict) Put(key string, val interface{}) (result int) {
	_, exists := sd.m.Load(key)
	sd.m.Store(key, val)
	if exists {
		return 0
	}
	return 1
}

func (sd *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, exists := sd.m.Load(key)
	if exists {
		return 0
	}
	sd.m.Store(key, val)
	return 1
}

func (sd *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, exists := sd.m.Load(key)
	if exists {
		sd.m.Store(key, val)
		return 1
	}
	return 0
}

func (sd *SyncDict) Remove(key string) (result int) {
	_, exists := sd.m.Load(key)
	sd.m.Delete(key)
	if exists {
		return 1
	}
	return 0
}

func (sd *SyncDict) ForEach(consumer Consumer) {
	sd.m.Range(func(key, value any) bool {
		consumer(key.(string), value)
		return true
	})
}

func (sd *SyncDict) Keys() []string {
	result := make([]string, sd.Len())
	i := 0
	sd.m.Range(func(key, value any) bool {
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

func (sd *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, sd.Len())
	for i := 0; i < 100; i++ {
		sd.m.Range(func(key, value any) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result
}

func (sd *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, sd.Len())
	i := 0
	sd.m.Range(func(key, value any) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return result
}

func (sd *SyncDict) Clear() {
	*sd = *MakeSyncDict()
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}
