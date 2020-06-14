package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"sort"
	"strings"

	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/pkg/stream"
)

type bitVector struct {
	numBits uint32
	bits    []uint64
}

func (v *bitVector) numWords() uint32 {
	wordSz := v.numBits / wordSize
	if v.numBits%wordSize != 0 {
		wordSz++
	}
	return wordSz
}
func (v *bitVector) String() string {
	var s strings.Builder
	for i := uint32(0); i < v.numBits; i++ {
		if readBit(v.bits, i) {
			s.WriteString("1")
		} else {
			s.WriteString("0")
		}
	}
	return s.String()
}

func (v *bitVector) bitsSize() uint32 {
	return v.numWords() * 8
}

func (v *bitVector) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	for _, n := range numBitsPerLevel {
		v.numBits += n
	}

	v.bits = make([]uint64, v.numWords())

	var wordID, bitShift uint32
	for level, bitsBlock := range bitsPerLevel {
		n := numBitsPerLevel[level]
		if n == 0 {
			continue
		}

		nCompleteWords := n / wordSize
		for word := 0; uint32(word) < nCompleteWords; word++ {
			v.bits[wordID] |= bitsBlock[word] << bitShift
			wordID++
			if bitShift > 0 {
				v.bits[wordID] |= bitsBlock[word] >> (wordSize - bitShift)
			}
		}

		remain := n % wordSize
		if remain > 0 {
			lastWord := bitsBlock[nCompleteWords]
			v.bits[wordID] |= lastWord << bitShift
			if bitShift+remain <= wordSize {
				bitShift = (bitShift + remain) % wordSize
				if bitShift == 0 {
					wordID++
				}
			} else {
				wordID++
				v.bits[wordID] |= lastWord >> (wordSize - bitShift)
				bitShift = bitShift + remain - wordSize
			}
		}
	}
}

func (v *bitVector) IsSet(pos uint32) bool {
	return readBit(v.bits, pos)
}

func (v *bitVector) DistanceToNextSetBit(pos uint32) uint32 {
	var distance uint32 = 1
	wordOff := (pos + 1) / wordSize
	bitsOff := (pos + 1) % wordSize

	if wordOff >= uint32(len(v.bits)) {
		return 0
	}

	testBits := v.bits[wordOff] >> bitsOff
	if testBits > 0 {
		return distance + uint32(bits.TrailingZeros64(testBits))
	}

	numWords := v.numWords()
	if wordOff == numWords-1 {
		return v.numBits - pos
	}
	distance += wordSize - bitsOff

	for wordOff < numWords-1 {
		wordOff++
		testBits = v.bits[wordOff]
		if testBits > 0 {
			return distance + uint32(bits.TrailingZeros64(testBits))
		}
		distance += wordSize
	}

	if wordOff == numWords-1 && v.numBits%64 != 0 {
		distance -= wordSize - v.numBits%64
	}

	return distance
}

type valueVector struct {
	bytes      []byte
	valueWidth uint32
}

func (v *valueVector) Init(valuesPerLevel [][]byte, valueWidth uint32) {
	var size int
	for l := range valuesPerLevel {
		size += len(valuesPerLevel[l])
	}
	v.valueWidth = valueWidth
	v.bytes = make([]byte, size)

	var pos uint32
	for _, val := range valuesPerLevel {
		copy(v.bytes[pos:], val)
		pos += uint32(len(val))
	}
}

func (v *valueVector) Get(pos uint32) []byte {
	off := pos * v.valueWidth
	return v.bytes[off : off+v.valueWidth]
}

func (v *valueVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *valueVector) rawMarshalSize() int64 {
	return 8 + int64(len(v.bytes))
}

func (v *valueVector) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], uint32(len(v.bytes)))
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}

	endian.PutUint32(bs[:], v.valueWidth)
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}

	if _, err := w.Write(v.bytes); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *valueVector) Unmarshal(buf []byte) ([]byte, error) {
	sr := stream.NewReader(buf)

	sz := sr.ReadUint32()
	v.valueWidth = sr.ReadUint32()
	v.bytes = sr.ReadSlice(int(sz))

	// read padding
	paddingWidth := align(int64(sr.Position())) - int64(sr.Position())
	_ = sr.ReadSlice(int(paddingWidth))

	return sr.UnreadSlice(), sr.Error()
}

const selectSampleInterval = 64

type selectVector struct {
	bitVector
	numOnes   uint32
	selectLut []uint32
}

func (v *selectVector) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *selectVector {
	v.bitVector.Init(bitsPerLevel, numBitsPerLevel)
	lut := []uint32{0}
	sampledOnes := selectSampleInterval
	onesUptoWord := 0
	for i, w := range v.bits {
		ones := bits.OnesCount64(w)
		for sampledOnes <= onesUptoWord+ones {
			diff := sampledOnes - onesUptoWord
			targetPos := i*wordSize + int(select64(w, int64(diff)))
			lut = append(lut, uint32(targetPos))
			sampledOnes += selectSampleInterval
		}
		onesUptoWord += ones
	}

	v.numOnes = uint32(onesUptoWord)
	v.selectLut = make([]uint32, len(lut))
	copy(v.selectLut, lut)

	return v
}

func (v *selectVector) lutSize() uint32 {
	return (v.numOnes/selectSampleInterval + 1) * 4
}

// Select returns the position of the rank-th 1 bit.
// position is zero-based; rank is one-based.
// E.g., for bitvector: 100101000, select(3) = 5
func (v *selectVector) Select(rank uint32) uint32 {
	lutIdx := rank / selectSampleInterval
	rankLeft := rank % selectSampleInterval
	if lutIdx == 0 {
		rankLeft--
	}

	pos := v.selectLut[lutIdx]
	if rankLeft == 0 {
		return pos
	}

	wordOff := pos / wordSize
	bitsOff := pos % wordSize
	if bitsOff == wordSize-1 {
		wordOff++
		bitsOff = 0
	} else {
		bitsOff++
	}

	w := v.bits[wordOff] >> bitsOff << bitsOff
	ones := uint32(bits.OnesCount64(w))
	for ones < rankLeft {
		wordOff++
		w = v.bits[wordOff]
		rankLeft -= ones
		ones = uint32(bits.OnesCount64(w))
	}

	return wordOff*wordSize + uint32(select64(w, int64(rankLeft)))
}

func (v *selectVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *selectVector) rawMarshalSize() int64 {
	return 4 + 4 + int64(v.bitsSize()) + int64(v.lutSize())
}

func (v *selectVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	_, err := w.Write(buf[:])
	if err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.numOnes)
	_, err = w.Write(buf[:])
	if err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.selectLut)); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err = w.Write(zeros[:padding])
	return err
}

func (v *selectVector) Unmarshal(buf []byte) ([]byte, error) {
	sr := stream.NewReader(buf)
	v.numBits = sr.ReadUint32()
	v.numOnes = sr.ReadUint32()
	v.bits = bytesToU64Slice(sr.ReadSlice(int(v.bitsSize())))
	v.selectLut = bytesToU32Slice(sr.ReadSlice(int(v.lutSize())))

	// read padding
	paddingWidth := align(int64(sr.Position())) - int64(sr.Position())
	_ = sr.ReadSlice(int(paddingWidth))
	return sr.UnreadSlice(), sr.Error()
}

const (
	rankSparseBlockSize = 512
)

type rankVector struct {
	bitVector
	blockSize uint32
	rankLut   []uint32
}

func (v *rankVector) init(blockSize uint32, bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *rankVector {
	v.bitVector.Init(bitsPerLevel, numBitsPerLevel)
	v.blockSize = blockSize
	wordPerBlk := v.blockSize / wordSize
	nblks := v.numBits/v.blockSize + 1
	v.rankLut = make([]uint32, nblks)

	var totalRank, i uint32
	for i = 0; i < nblks-1; i++ {
		v.rankLut[i] = totalRank
		totalRank += popcountBlock(v.bits, i*wordPerBlk, v.blockSize)
	}
	v.rankLut[nblks-1] = totalRank
	return v
}

func (v *rankVector) lutSize() uint32 {
	return (v.numBits/v.blockSize + 1) * 4
}

func (v *rankVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *rankVector) rawMarshalSize() int64 {
	return 4 + 4 + int64(v.bitsSize()) + int64(v.lutSize())
}

func (v *rankVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.blockSize)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.rankLut)); err != nil {
		return err
	}

	var zeros [8]byte
	padding := v.MarshalSize() - v.rawMarshalSize()
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *rankVector) Unmarshal(buf []byte) ([]byte, error) {
	sr := stream.NewReader(buf)
	v.numBits = sr.ReadUint32()
	v.blockSize = sr.ReadUint32()
	bitsSize := int(v.bitsSize())
	v.bits = bytesToU64Slice(sr.ReadSlice(bitsSize))

	lutSize := int(v.lutSize())
	v.rankLut = bytesToU32Slice(sr.ReadSlice(lutSize))

	// read padding
	paddingWidth := align(int64(sr.Position())) - int64(sr.Position())
	_ = sr.ReadSlice(int(paddingWidth))
	return sr.UnreadSlice(), sr.Error()
}

type rankVectorSparse struct {
	rankVector
}

func (v *rankVectorSparse) Init(bitsPerLevel [][]uint64, numBitsPerLevel []uint32) {
	v.rankVector.init(rankSparseBlockSize, bitsPerLevel, numBitsPerLevel)
}

func (v *rankVectorSparse) Rank(pos uint32) uint32 {
	wordPreBlk := uint32(rankSparseBlockSize / wordSize)
	blockOff := pos / rankSparseBlockSize
	bitsOff := pos % rankSparseBlockSize

	return v.rankLut[blockOff] + popcountBlock(v.bits, blockOff*wordPreBlk, bitsOff+1)
}

const labelTerminator = 0xff

type labelVector struct {
	labels []byte
}

func (v *labelVector) Init(labelsPerLevel [][]byte, endLevel uint32) {
	numBytes := 1
	for l := uint32(0); l < endLevel; l++ {
		numBytes += len(labelsPerLevel[l])
	}
	v.labels = make([]byte, numBytes)

	var pos uint32
	for l := uint32(0); l < endLevel; l++ {
		copy(v.labels[pos:], labelsPerLevel[l])
		pos += uint32(len(labelsPerLevel[l]))
	}
}

func (v *labelVector) GetLabel(pos uint32) byte {
	return v.labels[pos]
}

func (v *labelVector) Search(k byte, off, size uint32) (uint32, bool) {
	start := off
	if size > 1 && v.labels[start] == labelTerminator {
		start++
		size--
	}

	end := start + size
	if end > uint32(len(v.labels)) {
		end = uint32(len(v.labels))
	}
	result := bytes.IndexByte(v.labels[start:end], k)
	if result < 0 {
		return off, false
	}
	return start + uint32(result), true
}

func (v *labelVector) SearchGreaterThan(label byte, pos, size uint32) (uint32, bool) {
	if size > 1 && v.labels[pos] == labelTerminator {
		pos++
		size--
	}

	result := sort.Search(int(size), func(i int) bool { return v.labels[pos+uint32(i)] > label })
	if uint32(result) == size {
		return pos + uint32(result) - 1, false
	}
	return pos + uint32(result), true
}

func (v *labelVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *labelVector) rawMarshalSize() int64 {
	return 4 + int64(len(v.labels))
}

func (v *labelVector) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], uint32(len(v.labels)))
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if _, err := w.Write(v.labels); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *labelVector) Unmarshal(buf []byte) ([]byte, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("failed unmarsh labelVector, length: %d is too short", len(buf))
	}
	l := endian.Uint32(buf)
	tail := align(int64(4 + l))
	if tail > int64(len(buf)) {
		return nil, fmt.Errorf("failed unmarsh labelVector, offset:%d > %d ", tail, len(buf))
	}
	v.labels = buf[4 : 4+l]
	return buf[tail:], nil
}

type prefixVector struct {
	hasPrefixVec  rankVectorSparse
	prefixOffsets []uint32
	prefixData    []byte
}

func (v *prefixVector) Init(hasPrefixBits [][]uint64, numNodesPerLevel []uint32, prefixes [][][]byte) {
	v.hasPrefixVec.Init(hasPrefixBits, numNodesPerLevel)

	var offset uint32
	for _, level := range prefixes {
		for _, prefix := range level {
			v.prefixOffsets = append(v.prefixOffsets, offset)
			offset += uint32(len(prefix))
			v.prefixData = append(v.prefixData, prefix...)
		}
	}
}

func (v *prefixVector) CheckPrefix(key []byte, depth uint32, nodeID uint32) (uint32, bool) {
	prefix := v.GetPrefix(nodeID)
	if len(prefix) == 0 {
		return 0, true
	}

	if int(depth)+len(prefix) > len(key) {
		return 0, false
	}
	if !bytes.Equal(key[depth:depth+uint32(len(prefix))], prefix) {
		return 0, false
	}
	return uint32(len(prefix)), true
}

func (v *prefixVector) GetPrefix(nodeID uint32) []byte {
	if !v.hasPrefixVec.IsSet(nodeID) {
		return nil
	}

	prefixID := v.hasPrefixVec.Rank(nodeID) - 1
	start := v.prefixOffsets[prefixID]
	end := uint32(len(v.prefixData))
	if int(prefixID+1) < len(v.prefixOffsets) {
		end = v.prefixOffsets[prefixID+1]
	}
	return v.prefixData[start:end]
}

func (v *prefixVector) WriteTo(w io.Writer) error {
	if err := v.hasPrefixVec.WriteTo(w); err != nil {
		return err
	}

	var length [8]byte
	endian.PutUint32(length[:4], uint32(len(v.prefixOffsets)*4))
	endian.PutUint32(length[4:], uint32(len(v.prefixData)))

	if _, err := w.Write(length[:]); err != nil {
		return err
	}
	if _, err := w.Write(u32SliceToBytes(v.prefixOffsets)); err != nil {
		return err
	}
	if _, err := w.Write(v.prefixData); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *prefixVector) Unmarshal(b []byte) ([]byte, error) {
	buf1, err := v.hasPrefixVec.Unmarshal(b)
	if err != nil {
		return buf1, err
	}
	sr := stream.NewReader(buf1)
	offsetsLen := sr.ReadUint32()
	dataLen := sr.ReadUint32()

	v.prefixOffsets = bytesToU32Slice(sr.ReadSlice(int(offsetsLen)))
	v.prefixData = sr.ReadSlice(int(dataLen))
	// read padding
	paddingWidth := align(int64(sr.Position())) - int64(sr.Position())
	_ = sr.ReadSlice(int(paddingWidth))
	return sr.UnreadSlice(), sr.Error()
}

func (v *prefixVector) rawMarshalSize() int64 {
	return v.hasPrefixVec.MarshalSize() + 8 + int64(len(v.prefixOffsets)*4+len(v.prefixData))
}

func (v *prefixVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

// suffixKeyVector stores all remaining key-suffixes.
type suffixKeyVector struct {
	decoder         *encoding.FixedOffsetDecoder
	suffixesOffsets []byte
	suffixesBlock   []byte
}

func (v *suffixKeyVector) Init(offsetsPerLevel [][]int, data []byte) {
	v.suffixesBlock = data

	var size int
	for l := range offsetsPerLevel {
		size += len(offsetsPerLevel[l])
	}
	offsets := make([]int, size)[:0]

	for _, l := range offsetsPerLevel {
		offsets = append(offsets, l...)
	}
	encoder := encoding.NewFixedOffsetEncoder()
	encoder.FromValues(offsets)
	v.suffixesOffsets = encoder.MarshalBinary()
}

func (v *suffixKeyVector) getDecoder() *encoding.FixedOffsetDecoder {
	if v.decoder == nil {
		v.decoder = encoding.NewFixedOffsetDecoder(v.suffixesOffsets)
	}
	return v.decoder
}

func (v *suffixKeyVector) GetSuffix(valPos uint32) []byte {
	decoder := v.getDecoder()
	start, ok := decoder.Get(int(valPos))
	if !ok || start >= len(v.suffixesBlock) {
		return nil
	}

	length, width := binary.Uvarint(v.suffixesBlock[start:])
	if length == 0 {
		return nil
	}
	start += width
	end := start + int(length)
	if end > len(v.suffixesBlock) {
		return nil
	}

	return v.suffixesBlock[start:end]
}

func (v *suffixKeyVector) CheckSuffix(valPos uint32, key []byte, depth uint32) bool {
	suffix := v.GetSuffix(valPos)
	if depth >= uint32(len(key)) {
		return len(suffix) == 0
	}
	return bytes.Equal(suffix, key[depth:])
}

func (v *suffixKeyVector) rawMarshalSize() int64 {
	return int64(8 + len(v.suffixesOffsets) + len(v.suffixesBlock))
}

func (v *suffixKeyVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *suffixKeyVector) WriteTo(w io.Writer) error {
	var length [8]byte
	endian.PutUint32(length[:4], uint32(len(v.suffixesOffsets)))
	endian.PutUint32(length[4:], uint32(len(v.suffixesBlock)))

	if _, err := w.Write(length[:]); err != nil {
		return err
	}
	if _, err := w.Write(v.suffixesOffsets); err != nil {
		return err
	}
	if _, err := w.Write(v.suffixesBlock); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *suffixKeyVector) Unmarshal(b []byte) ([]byte, error) {
	sr := stream.NewReader(b)

	offsetsLen := sr.ReadUint32()
	blockLen := sr.ReadUint32()
	v.suffixesOffsets = sr.ReadSlice(int(offsetsLen))
	v.suffixesBlock = sr.ReadSlice(int(blockLen))
	// read padding
	paddingWidth := align(int64(sr.Position())) - int64(sr.Position())
	_ = sr.ReadSlice(int(paddingWidth))
	return sr.UnreadSlice(), sr.Error()
}
