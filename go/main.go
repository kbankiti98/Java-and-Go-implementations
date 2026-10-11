package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const workerCount = 3

type Task struct {
	ID          int
	Description string
}

type Result struct {
	TaskID      int
	Description string
	WorkerID    int
	Sequence    int64
}

// processTask simulates CPU/work time and returns an explicit error on bad input.
func processTask(task Task, workerID int, sequence *int64) (Result, error) {
	if strings.TrimSpace(task.Description) == "" {
		return Result{}, errors.New("task description must not be blank")
	}
	time.Sleep(time.Duration(250+(task.ID%3)*100) * time.Millisecond)
	return Result{
		TaskID:      task.ID,
		Description: task.Description,
		WorkerID:    workerID,
		Sequence:    atomic.AddInt64(sequence, 1),
	}, nil
}

func worker(id int, tasks <-chan Task, results chan<- Result, errs chan<- error,
	wg *sync.WaitGroup, sequence *int64) {
	defer wg.Done()
	logf("Worker-%d started.", id)

	for task := range tasks {
		logf("Worker-%d started Task-%d (%s)", id, task.ID, task.Description)
		result, err := processTask(task, id, sequence)
		if err != nil {
			errs <- fmt.Errorf("worker %d, task %d: %w", id, task.ID, err)
			continue
		}
		results <- result
		logf("Worker-%d completed Task-%d", id, task.ID)
	}
	logf("Worker-%d stopped.", id)
}

func logf(format string, args ...any) {
	fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func main() {
	taskQueue := make(chan Task, 6)
	results := make(chan Result, 6)
	errs := make(chan error, 6)
	var workers sync.WaitGroup
	var sequence int64

	outputPath := "go-results.txt"
	file, err := os.Create(outputPath)
	if err != nil {
		logf("ERROR: cannot create output file: %v", err)
		return
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			logf("ERROR: closing output file: %v", closeErr)
		}
	}()

	tasks := []Task{
		{ID: 1, Description: "Trip request validation"},
		{ID: 2, Description: "Driver matching"},
		{ID: 3, Description: "Fare estimation"},
		{ID: 4, Description: "Route update"},
		{ID: 5, Description: "Payment record processing"},
		{ID: 6, Description: "Trip summary generation"},
	}

	workers.Add(workerCount)
	for id := 1; id <= workerCount; id++ {
		go worker(id, taskQueue, results, errs, &workers, &sequence)
	}
	for _, task := range tasks {
		taskQueue <- task
	}
	close(taskQueue) // workers finish queued tasks, then exit

	go func() {
		workers.Wait()
		close(results)
		close(errs)
	}()

	writer := bufio.NewWriter(file)
	resultCount := 0
	for result := range results {
		line := fmt.Sprintf("Result-%d | Task-%d | %s | processed by Worker-%d",
			result.Sequence, result.TaskID, result.Description, result.WorkerID)
		if _, writeErr := writer.WriteString(line + "\n"); writeErr != nil {
			logf("ERROR: writing result: %v", writeErr)
			return
		}
		resultCount++
	}
	if flushErr := writer.Flush(); flushErr != nil {
		logf("ERROR: flushing output: %v", flushErr)
		return
	}
	for processErr := range errs {
		logf("ERROR: %v", processErr)
	}

	logf("All tasks queued: %d", len(tasks))
	logf("Results written: %d", resultCount)
	logf("Output file: %s", outputPath)
}
