package invertedindex

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestTagKVEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	entries := TagKVEntries{}
	assert.Equal(t, 0, entries.TagValuesCount())

	tagKVEntry := NewMockTagKVEntrySetINTF(ctrl)
	tagKVEntry.EXPECT().TagValuesCount().Return(10)
	entries = TagKVEntries{tagKVEntry}
	assert.Equal(t, 10, entries.TagValuesCount())

	zone, _, _ := buildInvertedIndexBlock()
	entry, err := newTagKVEntrySet(zone)
	assert.NoError(t, err)
	assert.Equal(t, 3, entry.TagValuesCount())
}

func Test_newTagKVEntrySet_error_cases(t *testing.T) {
	// block length too short, 8 bytes
	_, err := newTagKVEntrySet([]byte{16, 86, 104, 89, 32, 63, 84, 101})
	assert.NotNil(t, err)
	// validate offsets failure
	_, err = newTagKVEntrySet([]byte{
		1, 1, 1, 1,
		2, 2, 2, 2,
		3, 3, 3, 3,
		4, 4, 4, 4,
		5})
	assert.NotNil(t, err)
}

func Test_tagKVEntrySet_TrieTree_error_cases(t *testing.T) {
	zoneBlock, _, _ := buildInvertedIndexBlock()
	entrySetIntf, _ := newTagKVEntrySet(zoneBlock)
	entrySet := entrySetIntf.(*tagKVEntrySet)
	// read stream eof
	entrySet.sr.Reset([]byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 1, 1, 1, 1})
	// read stream eof
	_, err := entrySet.TrieTree()
	assert.NotNil(t, err)

	// failed validation of trie tree
	entrySet.sr.Reset([]byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 1, 1, 1, 1, 1, 1})
	_, err = entrySet.TrieTree()
	assert.NotNil(t, err)

	// LOUDS block unmarshal failed
	entrySet.sr.Reset([]byte{1, 2, 3, 4, 5, 6, 7, 8, 6, 1, 1, 1, 1, 1, 1})
	_, err = entrySet.TrieTree()
	assert.NotNil(t, err)

	// isPrefixKey block unmarshal failed
	out, _ := NewRankSelect().MarshalBinary()
	badBLOCK := append([]byte{1, 2, 3, 4, 5, 6, 7, 8,
		18,   // trie tree length
		1, 1, // labels
		1, 1, // is prefix
		13}) // louds

	badBLOCK = append(badBLOCK, out...) // LOUDS block
	entrySet.sr.Reset(badBLOCK)
	_, err = entrySet.TrieTree()
	assert.NotNil(t, err)
}
