package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

const defaultFormat = time.RFC3339

type TaskResult struct {
	ID       int64
	Created  time.Time
	Duration time.Duration
}

func (t TaskResult) String() string {
	return fmt.Sprintf("(Ok) Task %-19d: [Created: %s, Duration: %s]", t.ID, t.Created.Format(defaultFormat), t.Duration)
}

type TaskError struct {
	ID      int64
	Created time.Time
	Message string
}

func (e TaskError) Error() string {
	return fmt.Sprintf("(Err) Task %-19d, [Created: %s, Message: %s]", e.ID, e.Created.Format(defaultFormat), e.Message)
}

type TaskFunc func() (TaskResult, error)

// ============================================================================

func heavyTask(ctx context.Context, id int64, created time.Time) (TaskResult, error) {

	if time.Now().Nanosecond()/1000%2 > 0 {
		return TaskResult{}, TaskError{ID: id, Created: created, Message: "Bad nanoseconds"}
	}

	// --------------------------------------
	sleepTime := time.Duration(rand.Intn(135)+85) * time.Millisecond
	time.Sleep(sleepTime)
	// --------------------------------------

	duration := time.Since(created)

	newTask := TaskResult{
		ID:       id,
		Created:  created,
		Duration: duration,
	}

	if ctx.Err() != nil {
		return TaskResult{}, ctx.Err()
	}

	return newTask, nil
}

func taskProducer(ctx context.Context, tasksChan chan<- TaskFunc) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(tasksChan)
			return

		case <-ticker.C:
			newTask := func() (TaskResult, error) {
				return heavyTask(ctx, rand.Int63(), time.Now())
			}

			tasksChan <- newTask
		}
	}
}

func taskConsumer(
	tasksChan <-chan TaskFunc,
	resChan chan<- TaskResult,
	errChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for task := range tasksChan {
		res, err := task()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return
			}
			errChan <- err
			continue
		}

		resChan <- res
	}

}

func printErr(buf []error) {
	fmt.Println("Errors:")
	for _, err := range buf {
		fmt.Println(err)
	}
}

func printRes(buf []TaskResult) {
	fmt.Println("Done:")
	for _, res := range buf {
		fmt.Println(res)
	}
}

// ============================================================================

func main() {
	var approxBufSize = 256
	var resultBufSize = approxBufSize / 2
	var numWorkers = 1

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	var wg sync.WaitGroup

	tasksChan := make(chan TaskFunc, approxBufSize)
	resChan := make(chan TaskResult, resultBufSize)
	errChan := make(chan error, resultBufSize)

	// --------------------------------------

	numCPU := runtime.NumCPU()
	if numCPU > 2 {
		numWorkers = numCPU - 2
	}

	go taskProducer(ctx, tasksChan)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go taskConsumer(tasksChan, resChan, errChan, &wg)
	}

	// --------------------------------------

	resBuf := make([]TaskResult, 0, resultBufSize)
	errBuf := make([]error, 0, resultBufSize)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			close(resChan)
			close(errChan)
			fmt.Println("Finished.")
			return

		case <-ticker.C:
			printErr(errBuf)
			printRes(resBuf)
			errBuf = errBuf[:0]
			resBuf = resBuf[:0]

		case res := <-resChan:
			resBuf = append(resBuf, res)

		case err := <-errChan:
			errBuf = append(errBuf, err)

		}
	}

}
