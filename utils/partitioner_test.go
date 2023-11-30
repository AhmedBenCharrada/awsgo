package utils_test

import (
	"math"
	"testing"

	"github.com/AhmedBenCharrada/awsgo/utils"

	"github.com/stretchr/testify/assert"
)

func TestPartitions(t *testing.T) {
	testData := make([]int, 0, 110)
	for i := 0; i < 110; i++ {
		testData = append(testData, i)
	}

	t.Run("partition size > 0", func(t *testing.T) {
		partitionSize := 25
		expectedPartitionCount := getPartitionCount(len(testData), partitionSize)

		ch := utils.Partition(testData, partitionSize)

		partitionCount := 0
		for slice := range ch {
			partitionCount += 1
			if partitionCount <= len(testData)/partitionSize {
				assert.Equal(t, (partitionCount-1)*partitionSize, slice[0])
				assert.Equal(t, (partitionCount)*partitionSize-1, slice[len(slice)-1])
				continue
			}

			assert.Equal(t, (partitionCount-1)*partitionSize, slice[0])
			assert.Equal(t, testData[len(testData)-1], slice[len(slice)-1])
		}

		assert.Equal(t, expectedPartitionCount, partitionCount)
	})

	t.Run("partition size = 0", func(t *testing.T) {
		ch := utils.Partition(testData, 0)
		slice, isOpen := <-ch
		assert.False(t, isOpen)
		assert.Empty(t, slice)
	})
}

func getPartitionCount(size, partition int) int {
	if size%partition == 0 {
		return size / partition
	}

	c := math.Ceil(float64(size) / float64(partition))
	return int(c)
}
