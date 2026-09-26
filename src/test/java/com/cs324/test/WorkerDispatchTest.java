package com.cs324.test;

import com.cs324.common.ClientRemote;
import com.cs324.common.WorkerRemote;
import com.cs324.worker.WorkerRemoteImpl;
import com.cs324.worker.WorkerTaskCalculator;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Client -> coordinator job submission over real RMI.
 *
 * A 3-worker ring (1-3) elects worker 1 as coordinator; the client submits
 * MAX / PRIMESUM / PRIMECOUNT jobs. The coordinator splits each payload across
 * itself and its neighbours, aggregates the sub-results and delivers the final
 * answer to a recording ClientRemote. JAC is asserted coordinator-side: it
 * counts only assignments issued to OTHER workers.
 */
class WorkerDispatchTest {

    private static final int TEST_RMI_PORT = 19808;
    private static final String HOST = "localhost";
    private static final String CLIENT_NAME = "Client";
    private static final long AWAIT_TIMEOUT_MS = 10_000;

    private static Registry registry;

    private WorkerRemoteImpl w1;
    private WorkerRemoteImpl w2;
    private WorkerRemoteImpl w3;
    private RecordingClientRemote client;

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
        w1 = new WorkerRemoteImpl(1);
        w2 = new WorkerRemoteImpl(2);
        w3 = new WorkerRemoteImpl(3);
        client = new RecordingClientRemote();

        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        registry.rebind("Worker3", w3);
        registry.rebind(CLIENT_NAME, client);

        // Full ring so the coordinator knows every participant.
        w1.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        w1.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
        w2.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        w2.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
        w3.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        w3.connectAsNeighbour(2, HOST, TEST_RMI_PORT);

        // Elect worker 1 as the coordinator on every node.
        w1.receiveCoordinatorMessage(1, 100L);
        w2.receiveCoordinatorMessage(1, 101L);
        w3.receiveCoordinatorMessage(1, 102L);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
            }
        }
        for (WorkerRemoteImpl worker : new WorkerRemoteImpl[]{w1, w2, w3}) {
            if (worker != null) {
                worker.shutdown();
            }
        }
        unexportQuietly(w1);
        unexportQuietly(w2);
        unexportQuietly(w3);
        unexportQuietly(client);
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

    private void awaitResult(int jobId) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (client.received(jobId)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("job " + jobId + " never returned within " + AWAIT_TIMEOUT_MS + "ms");
    }

    @Test
    void maxIsCalculatedAcrossSplitPayloads() throws Exception {
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);
        assertEquals(22, client.resultFor(1));
    }

    @Test
    void primeSumIsAggregatedAcrossRanges() throws Exception {
        w1.submitJob(2, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, 20),
                HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(2);
        assertEquals(77L, client.resultFor(2));
    }

    @Test
    void primeCountIsAggregatedAcrossLists() throws Exception {
        w1.submitJob(3, "PRIMECOUNT", new int[]{2, 4, 6, 8, 10, 11, 13},
                HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(3);
        assertEquals(3, client.resultFor(3));
    }

    @Test
    void coordinatorJacCountsOnlyAssignmentsToOtherWorkers() throws Exception {
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);

        // Two of the three slices went to workers 2 and 3.
        assertEquals(2, w1.getJobAllocationCounter(),
                "coordinator JAC counts each assignment to another worker");
        assertEquals(0, w2.getJobAllocationCounter(),
                "a receiving worker never counts assigned work");
        assertEquals(0, w3.getJobAllocationCounter(),
                "a receiving worker never counts assigned work");
    }

    @Test
    void nonCoordinatorRejectsJobs() throws Exception {
        RemoteException thrown = assertThrows(RemoteException.class, () ->
                w2.submitJob(9, "MAX", new int[]{1, 2, 3}, HOST, TEST_RMI_PORT, CLIENT_NAME));
        assertEquals(0, client.callCount(),
                "no result may be delivered for a job the coordinator rejected");
    }

    @Test
    void deadParticipantSliceIsComputedLocally() throws Exception {
        // Sanity: a full job first, so the participant set is established.
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);
        assertEquals(22, client.resultFor(1));

        // Worker 2 "dies" (unexported) after the first job.
        UnicastRemoteObject.unexportObject(w2, true);

        w1.submitJob(5, "MAX", new int[]{30, 7, 1, 300, 12, 2}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(5);

        assertEquals(300, client.resultFor(5),
                "a dead participant's slice must be recomputed by the coordinator so the "
                        + "aggregate stays correct");
    }

    @Test
    void coordinatorRotatesAfterItsTermTrigger() throws Exception {
        // Each job assigns slices to workers 2 and 3: TWO JAC increments per job.
        // All workers start at JAC 0, so after the third job the coordinator hits
        // JAC 6 >= JOBS_PER_TERM(5) and must start a fresh election. With w1 now
        // on JAC 6 and w2/w3 still on 0, the lowest-JAC tie-break picks worker 3.
        for (int j = 1; j <= 3; j++) {
            w1.submitJob(100 + j, "MAX", new int[]{j, j + 7, 42}, HOST, TEST_RMI_PORT, CLIENT_NAME);
            awaitResult(100 + j);
        }

        final long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            if (w1.getCurrentCoordinatorId() != 1) {
                break;
            }
            Thread.sleep(50);
        }

        assertEquals(3, w1.getCurrentCoordinatorId(),
                "after JOBS_PER_TERM assignments the coordinator must rotate to the "
                        + "lowest-JAC worker (w3), got coordinator=" + w1.getCurrentCoordinatorId());
        assertEquals(6, w1.getJobAllocationCounter(),
                "the departed coordinator keeps the JAC it earned during its term");
    }
}