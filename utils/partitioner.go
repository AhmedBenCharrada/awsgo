package utils

// Partition partitions items into slices of the provided partition size.
func Partition[T any](items []T, partitionSize int) <-chan []T {
	ch := make(chan []T)
	go partition(items, partitionSize, ch)
	return ch
}

func partition[T any](items []T, partitionSize int, ch chan []T) {
	if partitionSize <= 0 {
		close(ch)
		return
	}

	size := len(items)
	partitionsCount := size / partitionSize

	i := 0
	for ; i < partitionsCount; i++ {
		ch <- items[(i * partitionSize):((i + 1) * partitionSize)]
	}

	if size%partitionSize != 0 {
		ch <- items[(i * partitionSize):size]
	}

	close(ch)
}
