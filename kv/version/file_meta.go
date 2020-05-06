package version

import (
	"fmt"

	"github.com/lindb/lindb/kv/table"
)

// FileMeta is the metadata for sst file
type FileMeta struct {
	fileNumber table.FileNumber // file number
	minKey     uint32           // min key
	maxKey     uint32           // max key
	fileSize   int32            // file size
}

// NewFileMeta new FileMeta instance
func NewFileMeta(fileNumber table.FileNumber, minKey uint32, maxKey uint32, fileSize int32) *FileMeta {
	return &FileMeta{
		fileNumber: fileNumber,
		minKey:     minKey,
		maxKey:     maxKey,
		fileSize:   fileSize,
	}
}

// GetFileNumber gets file number for sst file
func (f *FileMeta) GetFileNumber() table.FileNumber {
	return f.fileNumber
}

// GetMinKey gets min key in sst file
func (f *FileMeta) GetMinKey() uint32 {
	return f.minKey
}

// GetMaxKey gets max key in sst file
func (f *FileMeta) GetMaxKey() uint32 {
	return f.maxKey
}

// GetFileSize gets file size for sst file
func (f *FileMeta) GetFileSize() int32 {
	return f.fileSize
}

// String returns the string value of file meta
func (f *FileMeta) String() string {
	return fmt.Sprintf("{fileNumber:%d,min:%d,max:%d,size:%d}", f.fileNumber, f.minKey, f.maxKey, f.fileSize)
}
