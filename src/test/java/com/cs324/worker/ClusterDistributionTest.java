package com.cs324.worker;

import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.common.BootstrapRemote;
import com.cs324.common.TaskAssignment;
import com.cs324.common.WorkerRemote;
import com.cs324.test.RecordingClientRemote;
import com.cs324.worker.WorkerTaskCalculator.PrimeRange;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClusterDistributionTest {
    private static final int PORT = 19809;
    private Registry registry;
    private BootstrapRegistryImpl directory;
    private BootstrapRemote bootstrap;
    private final List<WorkerRemoteImpl> workers = new ArrayList<>();
    private RecordingClientRemote firstClient, secondClient;
    private WorkerRemote coordinator;
    private DelayedWorker delayed;

    private static class DelayedWorker extends WorkerRemoteImpl {
        volatile CountDownLatch arrived = new CountDownLatch(0);
        volatile CountDownLatch release = new CountDownLatch(0);
        volatile Object lastPayload;
        DelayedWorker(int id) throws RemoteException { super(id); }
        void hold(int calls) {
            arrived = new CountDownLatch(calls);
            release = new CountDownLatch(1);
        }
        @Override public void assignTask(int jobId, String type, Object payload) throws RemoteException {
            lastPayload = payload;
            arrived.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new RemoteException("Test release timed out");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RemoteException("Interrupted", e);
            }
            super.assignTask(jobId, type, payload);
        }
    }

    @BeforeEach void setup() throws Exception {
        registry = LocateRegistry.createRegistry(PORT);
        directory = new BootstrapRegistryImpl();
        registry.rebind("BootstrapRegistry", directory);
        bootstrap = (BootstrapRemote) LocateRegistry.getRegistry("localhost", PORT).lookup("BootstrapRegistry");
        for (int id = 1; id <= 4; id++) {
            WorkerRemoteImpl worker = id == 3 ? (delayed = new DelayedWorker(id)) : new WorkerRemoteImpl(id);
            workers.add(worker);
            registry.rebind("Worker" + id, worker);
            bootstrap.registerWorker(id, "localhost", PORT);
            worker.setBootstrapReference(bootstrap, "localhost");
            if (id > 1) {
                worker.connectAsNeighbour(id - 1, "localhost", PORT);
                workers.get(id - 2).connectAsNeighbour(id, "localhost", PORT);
            }
        }
        workers.getFirst().startElection();
        assertEquals(4, workers.getLast().getCurrentCoordinatorId());
        coordinator = (WorkerRemote) LocateRegistry.getRegistry("localhost", PORT).lookup("Worker4");
        firstClient = new RecordingClientRemote();
        secondClient = new RecordingClientRemote();
        registry.rebind("ClientA", firstClient);
        registry.rebind("ClientB", secondClient);
    }

    @AfterEach void cleanup() throws Exception {
        if (delayed != null) delayed.release.countDown();
        for (var worker : workers) {
            worker.shutdown();
            UnicastRemoteObject.unexportObject(worker, true);
        }
        UnicastRemoteObject.unexportObject(firstClient, true);
        UnicastRemoteObject.unexportObject(secondClient, true);
        UnicastRemoteObject.unexportObject(directory, true);
        UnicastRemoteObject.unexportObject(registry, true);
    }

    private Object result(RecordingClientRemote client, int jobId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!client.received(jobId) && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(client.received(jobId), "Missing final result for " + jobId);
        return client.resultFor(jobId);
    }

    @Test void allFourWorkersParticipateWithoutChangingElectionNeighbours() throws Exception {
        int job = bootstrap.allocateJobId();
        coordinator.submitJob(job, "MAX", new int[]{1,2,3,4,5,6,7,8}, "localhost", PORT, "ClientA");
        assertEquals(8, result(firstClient, job));
        assertEquals(Map.of(4, 2, 1, 4, 2, 6, 3, 8), workers.getLast().getJobResults(job));
        assertEquals(java.util.Set.of(3), workers.getLast().getNeighbourIds());
        assertEquals(3, coordinator.getJobAllocationCounter());
        assertEquals(0, workers.getFirst().getJobAllocationCounter());
    }

    @Test void assignmentExampleUsesFourInclusiveRangesAndReturns76127() throws Exception {
        int job = bootstrap.allocateJobId();
        coordinator.submitJob(job, "PRIMESUM", new PrimeRange(1,1000), "localhost", PORT, "ClientA");
        assertEquals(76127L, result(firstClient, job));
        assertEquals(List.of(new PrimeRange(1,250), new PrimeRange(251,500),
                new PrimeRange(501,750), new PrimeRange(751,1000)),
                WorkerTaskDistributor.split("PRIMESUM", new PrimeRange(1,1000), 4));
        assertEquals(4, workers.getLast().getJobResults(job).size());
    }

    @Test void waitsForLastWorkerAndReturnsToOriginalCoordinatorAfterLeaderChange() throws Exception {
        int job = bootstrap.allocateJobId();
        delayed.hold(1);
        try (var pool = Executors.newSingleThreadExecutor()) {
            var submission = pool.submit(() -> {
                coordinator.submitJob(job, "MAX", new int[]{1,2,3,4,5,6,7,99}, "localhost", PORT, "ClientA");
                return null;
            });
            try {
                assertTrue(delayed.arrived.await(5, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (workers.getLast().getJobResults(job).size() < 3 && System.nanoTime() < deadline) Thread.sleep(10);
                assertEquals(3, workers.getLast().getJobResults(job).size());
                assertFalse(firstClient.received(job), "Must wait for the last chunk");
                workers.getFirst().receiveCoordinatorMessage(2, 99999999L);
                assertTrue(delayed.lastPayload instanceof TaskAssignment);
            } finally { delayed.release.countDown(); }
            submission.get(5, TimeUnit.SECONDS);
        }
        assertEquals(99, result(firstClient, job));
        assertEquals(1, firstClient.callCount());
    }

    @Test void twoConcurrentClientsHaveUniqueJobsAndSeparateCallbacks() throws Exception {
        int a, b;
        delayed.hold(2);
        try (var pool = Executors.newFixedThreadPool(2)) {
            var first = pool.submit(() -> {
                int job = bootstrap.allocateJobId();
                coordinator.submitJob(job, "MAX", new int[]{1,2,3,4}, "localhost", PORT, "ClientA");
                return job;
            });
            var second = pool.submit(() -> {
                BootstrapRemote otherConnection = (BootstrapRemote) LocateRegistry.getRegistry("localhost", PORT).lookup("BootstrapRegistry");
                int job = otherConnection.allocateJobId();
                coordinator.submitJob(job, "PRIMECOUNT", new int[]{2,7,5,5,11,13,4,8}, "localhost", PORT, "ClientB");
                return job;
            });
            try { assertTrue(delayed.arrived.await(5, TimeUnit.SECONDS)); }
            finally { delayed.release.countDown(); }
            a = first.get(5, TimeUnit.SECONDS);
            b = second.get(5, TimeUnit.SECONDS);
        }
        assertNotEquals(a,b);
        assertEquals(4, result(firstClient,a));
        assertEquals(6, result(secondClient,b));
        assertFalse(firstClient.received(b));
        assertFalse(secondClient.received(a));
    }
}
