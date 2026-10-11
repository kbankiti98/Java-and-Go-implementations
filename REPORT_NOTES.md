# Report Notes: Concurrency and Error Handling

Java uses a thread-safe `LinkedBlockingQueue` to coordinate access to pending tasks. A fixed-size `ExecutorService` runs three worker tasks. Workers block on `take()` instead of repeatedly polling an empty queue, and a shutdown sentinel per worker makes termination explicit. Results are collected in a synchronized list. `InterruptedException` is caught and the interrupt flag is restored; file failures are handled with `IOException`. Try-with-resources closes the output writer reliably.

Go uses goroutines as workers and channels as the shared task queue and result stream. Closing the task channel signals that no more tasks will be sent while allowing buffered tasks to be processed. A `sync.WaitGroup` coordinates worker completion, and the results channel is closed only after workers finish. Go functions return errors explicitly; callers check errors, and `defer` closes the output file.

The key distinction is that Java commonly combines shared-memory threads with synchronization utilities, while Go emphasizes lightweight goroutines communicating through channels. Both implementations avoid holding a shared lock during simulated processing, log worker lifecycle events, and use explicit shutdown coordination to reduce deadlock risk.
