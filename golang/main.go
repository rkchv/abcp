package main

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

const (
	TimeFormat      = time.RFC3339
	ApproxBufSize   = 256
	ResultBufSize   = ApproxBufSize / 2
	TotalWorkTime   = 10 * time.Second
	NewTaskInterval = 100 * time.Millisecond
	PrintInterval   = 3 * time.Second
)

// ============================================================================

type Task struct {
	ID      int64
	Created time.Time
}

type TaskResult struct {
	Task
	Duration time.Duration
}

type TaskError struct {
	Task
	Message string
}

func NewTask(id int64, created time.Time) Task {
	return Task{ID: id, Created: created}
}

func (t Task) Exec() (TaskResult, error) {
	if time.Now().Nanosecond()/1000%2 > 0 {
		return TaskResult{}, TaskError{Task: Task{ID: t.ID, Created: t.Created}, Message: "Bad Nanoseconds"}
	}

	sleepTime := time.Duration(rand.Intn(135)+85) * time.Millisecond
	time.Sleep(sleepTime)

	duration := time.Since(t.Created)

	res := TaskResult{
		Task:     Task{ID: t.ID, Created: t.Created},
		Duration: duration,
	}

	return res, nil
}

func (t TaskResult) String() string {
	return fmt.Sprintf("(Ok) Task %-19d: [Created: %s, Duration: %s]", t.ID, t.Created.Format(TimeFormat), t.Duration)
}

func (e TaskError) Error() string {
	return fmt.Sprintf("(Err) Task %-19d, [Created: %s, Message: %s]", e.ID, e.Created.Format(TimeFormat), e.Message)
}

// ============================================================================

func taskProducer(
	ctx context.Context,
	tasksChan chan<- Task,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	ticker := time.NewTicker(NewTaskInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			newTask := NewTask(rand.Int63(), time.Now())

			select {
			case tasksChan <- newTask:
			default:
			}
		}
	}
}

func taskWorker(
	ctx context.Context,
	tasksChan <-chan Task,
	resChan chan<- TaskResult,
	errChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for task := range tasksChan {
		res, err := task.Exec()

		if ctx.Err() != nil {
			return
		}

		if err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), TotalWorkTime)
	defer cancel()

	ticker := time.NewTicker(PrintInterval)
	defer ticker.Stop()

	var wg sync.WaitGroup

	tasksChan := make(chan Task, ApproxBufSize)
	resChan := make(chan TaskResult, ResultBufSize)
	errChan := make(chan error, ResultBufSize)

	// --------------------------------------

	numCPU := runtime.NumCPU()
	numWorkers := 1
	if numCPU > 2 {
		numWorkers = numCPU - 2
	}

	wg.Add(1)
	go taskProducer(ctx, tasksChan, &wg)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go taskWorker(ctx, tasksChan, resChan, errChan, &wg)
	}

	// --------------------------------------

	resBuf := make([]TaskResult, 0, ResultBufSize)
	errBuf := make([]error, 0, ResultBufSize)

	for {
		select {
		case <-ctx.Done():
      			close(tasksChan)
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
