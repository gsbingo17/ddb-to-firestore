package firestore

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// WriteJob represents a unit of work for the Firestore writer pool
type WriteJob struct {
	Database   string
	Collection string
	Operations []Operation
	ResultChan chan error // Channel to send the result back to the caller
}

// WriterPool manages concurrent writes to Firestore
type WriterPool struct {
	client    *Client
	logger    *zap.Logger
	workers   chan struct{} // Semaphore to limit concurrent workers
	jobQueue  chan WriteJob // Channel for incoming write jobs
	wg        sync.WaitGroup
	closeOnce sync.Once
	closed    chan struct{} // Signaled when the pool is closing
}

// NewWriterPool creates and starts a new WriterPool
func NewWriterPool(client *Client, numWorkers int, logger *zap.Logger) *WriterPool {
	pool := &WriterPool{
		client:   client,
		logger:   logger,
		workers:  make(chan struct{}, numWorkers),
		jobQueue: make(chan WriteJob),
		closed:   make(chan struct{}),
	}

	for i := 0; i < numWorkers; i++ {
		pool.wg.Add(1)
		go pool.worker(i + 1)
	}

	logger.Info("Firestore writer pool started", zap.Int("numWorkers", numWorkers))
	return pool
}

// Submit adds a WriteJob to the pool's queue
func (wp *WriterPool) Submit(job WriteJob) {
	select {
	case wp.jobQueue <- job:
		// Job submitted successfully
	case <-wp.closed:
		// Pool is closing, return error immediately
		job.ResultChan <- fmt.Errorf("writer pool is closed, cannot submit job")
	}
}

// worker is a goroutine that processes jobs from the queue
func (wp *WriterPool) worker(id int) {
	defer wp.wg.Done()
	wp.logger.Debug("Writer pool worker started", zap.Int("workerId", id))

	for {
		select {
		case job, ok := <-wp.jobQueue:
			if !ok {
				wp.logger.Debug("Writer pool job queue closed, worker shutting down", zap.Int("workerId", id))
				return // Channel closed, no more jobs
			}

			wp.workers <- struct{}{} // Acquire worker slot

			err := wp.client.ExecuteBatch(context.Background(), job.Database, job.Collection, job.Operations)

			job.ResultChan <- err // Send result back to caller
			<-wp.workers          // Release worker slot

		case <-wp.closed:
			wp.logger.Debug("Writer pool received close signal, worker shutting down", zap.Int("workerId", id))
			return
		}
	}
}

// Close gracefully shuts down the WriterPool
func (wp *WriterPool) Close() {
	wp.closeOnce.Do(func() {
		wp.logger.Info("Shutting down Firestore writer pool...")
		close(wp.closed)   // Signal workers to stop
		close(wp.jobQueue) // Close job queue to prevent new jobs

		wp.wg.Wait() // Wait for all workers to finish current jobs
		wp.logger.Info("Firestore writer pool shut down successfully.")
	})
}
