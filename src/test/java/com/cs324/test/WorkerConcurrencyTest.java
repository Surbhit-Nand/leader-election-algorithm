package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Group 6: concurrency / thread-safety tests for worker-side job execution.
 *
 * Covers the acceptance checklist without touching election semantics:
 *  - multiple jobs run concurrently and all complete (no head-of-line blocking)
 *  - duplicate assignTask(jobId) collapses to a single execution (no double work)
 *  - concurrent reportTaskResult() calls for the SAME jobId from DIFFERENT
 *    workers are all retained (no lost updates)
 *  - concurrent startElection() calls still produce unique message IDs
 *
 * JAC note: per the spec and the Design Decisions note, JAC counts assignments
 * issued by a coordinator to other workers, so a worker RECEIVING assignTask()
 * does not increment its own counter (that happens on the coordinator in
 * submitJob()). These tests therefore assert the receiver's JAC stays 0 while
 * still verifying exactly-once execution and concurrency safety.
 */
class WorkerConcurrencyTest {

    private static final int TEST_RMI_PORT = 19807;
    private static final String HOST = "localhost";
    private static final int COORDINATOR_ID = 9;
    private static final int THIS_WORKER_ID = 1;
    private static final long AWAIT_TIMEOUT_MS = 10_000;

    private static Registry registry;

    private WorkerRemoteImpl worker;
    private RecordingWorkerRemote coordinator;

    @BeforeAll
    static void startRegistry() throws RemoteException {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @BeforeEach
    void setUp() throws Exception {
        worker = new WorkerRemoteImpl(THIS_WORKER_ID);
        coordinator = new RecordingWorkerRemote(COORDINATOR_ID);

        registry.rebind("Worker" + COORDINATOR_ID, coordinator);

        worker.connectAsNeighbour(COORDINATOR_ID, HOST, TEST_RMI_PORT);
        worker.receiveCoordinatorMessage(COORDINATOR_ID, 777L);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
            }
        }
        if (worker != null) {
            worker.shutdown();
        }
        unexportQuietly(worker);
        unexportQuietly(coordinator);
    }

    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
        }
    }

    private void awaitReportedResult(int jobId) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (coordinator.receivedResult(jobId, THIS_WORKER_ID)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("job " + jobId + " never reported within " + AWAIT_TIMEOUT_MS + "ms; recorded: "
                + coordinator.calls());
    }

    @Test
    void concurrentDistinctJobsAllCompleteWithoutBlockingEachOther() throws Exception {
        int jobs = 20;
        ExecutorService submitters = Executors.newFixedThreadPool(8);
        try {
            for (int j = 0; j < jobs; j++) {
                final int jobId = 1000 + j;
                submitters.submit(() -> {
                    try {
                        worker.assignTask(jobId, "MAX", new int[]{jobId, jobId + 1, jobId - 1});
                    } catch (RemoteException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            submitters.shutdown();
            assertTrue(submitters.awaitTermination(5, TimeUnit.SECONDS), "submitters hung");
        } finally {
            submitters.shutdownNow();
        }

        // assignTask must be fire-and-return: all 20 submissions above complete fast,
        // then every background computation reports back independently.
        for (int j = 0; j < jobs; j++) {
            awaitReportedResult(1000 + j);
        }
        for (int j = 0; j < jobs; j++) {
            assertEquals(String.valueOf(1000 + j + 1),
                    coordinator.lastReportedResult(1000 + j, THIS_WORKER_ID),
                    "wrong MAX result for job " + (1000 + j));
        }
        // Assigning work is fire-and-return; the receiver never counts it.
        // JAC is a coordinator-only counter (see submitJob()).
        assertEquals(0, worker.getJobAllocationCounter(),
                "a worker that receives assignTask() must not increment its own JAC; "
                        + "JAC counts coordinator-side assignments only");
    }

    @Test
    void duplicateConcurrentSubmissionsOfTheSameJobExecuteOnlyOnce() throws Exception {
        int duplicates = 10;
        int jobId = 2000;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService submitters = Executors.newFixedThreadPool(duplicates);
        try {
            for (int i = 0; i < duplicates; i++) {
                submitters.submit(() -> {
                    try {
                        start.await();
                        worker.assignTask(jobId, "MAX", new int[]{4, 11, 2, 19, 8});
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            start.countDown(); // release all submitters at once for maximum contention
            submitters.shutdown();
            assertTrue(submitters.awaitTermination(5, TimeUnit.SECONDS), "submitters hung");
        } finally {
            submitters.shutdownNow();
        }

        awaitReportedResult(jobId);
        // Give any stray duplicate execution a chance to (incorrectly) report again.
        Thread.sleep(300);

        long reports = coordinator.calls().stream()
                .filter(c -> c.startsWith("result|" + jobId + "|" + THIS_WORKER_ID + "|"))
                .count();
        assertEquals(1L, reports,
                "duplicate assignTask calls must collapse to one execution, otherwise work is "
                        + "duplicated; recorded: " + coordinator.calls());
        assertEquals(0, worker.getJobAllocationCounter(),
                "a receiver of assignTask() must not count the job; JAC is coordinator-only");
        assertEquals("19", coordinator.lastReportedResult(jobId, THIS_WORKER_ID));
    }

    @Test
    void concurrentResultsForTheSameJobFromDifferentWorkersAreAllRetained() throws Exception {
        int jobId = 3000;
        int reporters = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(reporters);
        try {
            for (int w = 0; w < reporters; w++) {
                final int workerId = 50 + w;
                pool.submit(() -> {
                    try {
                        start.await();
                        worker.reportTaskResult(jobId, workerId, "r-" + workerId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "reporters hung");
        } finally {
            pool.shutdownNow();
        }

        Map<Integer, Object> results = worker.getJobResults(jobId);
        assertEquals(reporters, results.size(),
                "concurrent per-worker results for one job must not overwrite each other: " + results);
        for (int w = 0; w < reporters; w++) {
            assertEquals("r-" + (50 + w), String.valueOf(results.get(50 + w)));
        }
    }

    @Test
    void concurrentElectionsProduceUniqueMessageIds() throws Exception {
        int elections = 10;
        ExecutorService pool = Executors.newFixedThreadPool(elections);
        Set<Long> ids = java.util.Collections.synchronizedSet(new HashSet<>());
        try {
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < elections; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        ids.add(worker.startElection());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "election starters hung");
        } finally {
            pool.shutdownNow();
        }
        assertEquals(elections, ids.size(), "every election wave needs a unique message ID, got: " + ids);
    }
}
