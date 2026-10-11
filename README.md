# Parallel Data Processing System (Java and Go)

A ride-sharing-inspired concurrent worker pipeline. Three workers process six sample tasks, simulate processing time, log lifecycle events, and save results to output files.

## Project layout

- `java/DataProcessingSystem.java` — Java implementation using `BlockingQueue`, `ExecutorService`, synchronized result collection, stop tokens, and try-with-resources.
- `go/main.go` — Go implementation using goroutines, channels, `sync.WaitGroup`, explicit error checks, and `defer`.
- `REPORT_NOTES.md` — concise explanation for the assignment report.

## Requirements

- Java 17 or newer (uses `Path.of` and modern language/library APIs).
- Go 1.21 or newer.

## Run Java

```bash
cd java
javac DataProcessingSystem.java
java DataProcessingSystem
```

Output is printed to the terminal and saved to `java-results.txt`.

## Run Go

```bash
cd go
go run main.go
```

Output is printed to the terminal and saved to `go-results.txt`.

## Concurrency and safe shutdown

The Java queue is a `LinkedBlockingQueue`, so queue operations are thread-safe and `take()` blocks instead of busy-waiting. A shutdown sentinel is inserted for each worker. The executor is shut down and awaited, and the results are written after workers finish.

The Go task channel distributes tasks to goroutines. Closing the channel tells workers that no further tasks will arrive; workers drain queued tasks before returning. A `WaitGroup` tracks completion, and a coordinator closes the results and error channels only after all workers finish.

## Notes

Task completion order is nondeterministic because workers run concurrently. The result sequence reflects completion order, not task ID order. The sample tasks are deterministic, but real production systems should consider cancellation, timeouts, retry policies, structured logging, and durable output handling.
