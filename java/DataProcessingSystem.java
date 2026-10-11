import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ride-sharing-inspired parallel data processing system.
 * Compile: javac DataProcessingSystem.java
 * Run:     java DataProcessingSystem
 */
public class DataProcessingSystem {
    private static final int WORKER_COUNT = 3;
    private static final Path OUTPUT_FILE = Path.of("java-results.txt");
    private static final AtomicInteger RESULT_SEQUENCE = new AtomicInteger(1);

    static final class Task {
        final int id;
        final String description;

        Task(int id, String description) {
            this.id = id;
            this.description = description;
        }

        @Override
        public String toString() {
            return "Task-" + id + " (" + description + ")";
        }
    }

    /** Thread-safe task queue. Blocking take avoids polling an empty queue. */
    static final class SharedTaskQueue {
        private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();

        void addTask(Task task) {
            if (task == null) {
                throw new IllegalArgumentException("Task cannot be null");
            }
            queue.add(task);
        }

        Task getTask() throws InterruptedException {
            return queue.take();
        }
    }

    static final class Worker implements Runnable {
        private final int workerId;
        private final SharedTaskQueue taskQueue;
        private final List<String> results;
        private final Object resultLock;

        Worker(int workerId, SharedTaskQueue taskQueue,
               List<String> results, Object resultLock) {
            this.workerId = workerId;
            this.taskQueue = taskQueue;
            this.results = results;
            this.resultLock = resultLock;
        }

        @Override
        public void run() {
            log("Worker-" + workerId + " started.");
            try {
                while (true) {
                    Task task = taskQueue.getTask();
                    if (task.id == -1) {
                        log("Worker-" + workerId + " received shutdown signal.");
                        break;
                    }

                    log("Worker-" + workerId + " started " + task);
                    String result = processTask(task);
                    synchronized (resultLock) {
                        results.add(result);
                    }
                    log("Worker-" + workerId + " completed Task-" + task.id);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logError("Worker-" + workerId + " interrupted; stopping safely.", e);
            } catch (RuntimeException e) {
                logError("Worker-" + workerId + " encountered a processing error.", e);
            }
            log("Worker-" + workerId + " stopped.");
        }

        private String processTask(Task task) throws InterruptedException {
            // Simulate computational work.
            Thread.sleep(250L + (task.id % 3) * 100L);
            if (task.description == null || task.description.isBlank()) {
                throw new IllegalArgumentException("Task description must not be blank");
            }
            return String.format("Result-%d | %s | processed by Worker-%d",
                    RESULT_SEQUENCE.getAndIncrement(), task.description, workerId);
        }
    }

    public static void main(String[] args) {
        SharedTaskQueue queue = new SharedTaskQueue();
        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Object resultLock = new Object();
        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);

        List<Task> tasks = List.of(
                new Task(1, "Trip request validation"),
                new Task(2, "Driver matching"),
                new Task(3, "Fare estimation"),
                new Task(4, "Route update"),
                new Task(5, "Payment record processing"),
                new Task(6, "Trip summary generation")
        );

        try {
            Files.deleteIfExists(OUTPUT_FILE);
            for (Task task : tasks) {
                queue.addTask(task);
            }
            // One sentinel per worker guarantees that each worker can terminate.
            for (int i = 0; i < WORKER_COUNT; i++) {
                queue.addTask(new Task(-1, "shutdown"));
            }

            for (int i = 1; i <= WORKER_COUNT; i++) {
                executor.submit(new Worker(i, queue, results, resultLock));
            }

            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                logError("Workers did not stop within the timeout; forcing shutdown.", null);
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    logError("Executor did not terminate after forced shutdown.", null);
                }
            }

            try (BufferedWriter writer = Files.newBufferedWriter(
                    OUTPUT_FILE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                synchronized (results) {
                    for (String result : results) {
                        writer.write(result);
                        writer.newLine();
                    }
                }
            }

            log("All tasks queued: " + tasks.size());
            log("Results written: " + results.size());
            log("Output file: " + OUTPUT_FILE.toAbsolutePath());
        } catch (IOException e) {
            logError("File I/O failure.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logError("Main thread interrupted while waiting for workers.", e);
            executor.shutdownNow();
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
    }

    private static void log(String message) {
        String time = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        System.out.println("[" + time + "] " + message);
    }

    private static void logError(String message, Exception error) {
        log("ERROR: " + message + (error == null ? "" : " Details: " + error.getMessage()));
    }
}
