package consistenthash

import (
	"hash/crc32"
	"sort"
)

// HashFunc 定义生成哈希码的函数类型
type HashFunc func(data []byte) uint32

// NodeMap 存储节点，可以从 NodeMap 中选择节点
type NodeMap struct {
	hashFunc   HashFunc       // 哈希函数
	nodeHashes []int          // 已排序的各个节点哈希码
	nodeMap    map[int]string // 节点哈希码到节点名称的映射
}

// NewNodeMap 创建一个新的 NodeMap
func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc: fn,
		nodeMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty 返回 NodeMap 是否为空
func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashes) == 0
}

// AddNode 将给定的节点添加到一致性哈希环中
func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashes = append(m.nodeHashes, hash)
		m.nodeMap[hash] = key
	}
	sort.Ints(m.nodeHashes)
}

// PickNode 获取哈希环中与提供的键最接近的节点
func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hashFunc([]byte(key)))

	// 查找找到合适的节点
	idx := sort.Search(len(m.nodeHashes), func(i int) bool {
		return m.nodeHashes[i] >= hash
	})

	// 未找到，循环回到第一个副本
	if idx == len(m.nodeHashes) {
		idx = 0
	}

	return m.nodeMap[m.nodeHashes[idx]]
}
