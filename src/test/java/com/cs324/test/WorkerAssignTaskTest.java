package com.cs324.test;

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
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Step 7 of the worker test plan: assignTask().
 *
 * assignTask is deliberately fire-and-return: the coordinator hands over a slice
 * of work and immediately carries on, so the worker computes on a background
 * thread and calls reportTaskResult back on the coordinator when it is done.
 *
 * These tests pin the three calculations the assignment requires (MAX,
 * PRIMESUM over a range, PRIMECOUNT over a list) and prove that the result is
 * actually delivered to the coordinator over RMI.
 *
 * The JAC is deliberately NOT asserted here: JAC counts coordinator-side
 * assignments (see submitJob()) and is covered by WorkerDispatchTest.
 */
class WorkerAssignTaskTest {

    private static final int TEST_RMI_PORT = 19806;
    private static final String HOST = "localhost";
    private static final int COORDINATOR_ID = 9;
    private static final int THIS_WORKER_ID = 1;
    private static final long AWAIT_TIMEOUT_MS = 5_000;

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

        // Make the recording peer this worker's neighbour AND its coordinator, so
        // the completion callback has somewhere real to go.
        worker.connectAsNeighbour(COORDINATOR_ID, HOST, TEST_RMI_PORT);
        worker.receiveCoordinatorMessage(COORDINATOR_ID, 1L);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
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
            // already unexported, nothing to do
        }
    }

    /** Waits for the background computation to report back, and returns the value. */
    private String awaitReportedResult(int jobId) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            if (coordinator.receivedResult(jobId, THIS_WORKER_ID)) {
                return coordinator.lastReportedResult(jobId, THIS_WORKER_ID);
            }
            Thread.sleep(20);
        }

        fail("assignTask returned but never reported a result for job " + jobId + " within "
                + AWAIT_TIMEOUT_MS + "ms - the background task either never ran or could not reach the "
                + "coordinator; recorded: " + coordinator.calls());
        return null;
    }

    @Test
    void reportsTheMaximumOfTheAssignedNumbers() throws Exception {
        worker.assignTask(1, "MAX", new int[]{4, 11, 2, 19, 8});

        assertEquals("19", awaitReportedResult(1),
                "MAX(4, 11, 2, 19, 8) is 19");
    }

    @Test
    void reportsTheSumOfPrimesOverTheAssignedRange() throws Exception {
        worker.assignTask(2, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, 10));

        assertEquals("17", awaitReportedResult(2),
                "PRIMESUM(1, 10) covers 2, 3, 5 and 7, which sum to 17");
    }

    @Test
    void reportsHowManyPrimesAreInTheAssignedList() throws Exception {
        worker.assignTask(3, "PRIMECOUNT", new int[]{2, 8, 7, 4, 11, 10});

        assertEquals("3", awaitReportedResult(3),
                "PRIMECOUNT(2, 8, 7, 4, 11, 10) sees 2, 7 and 11");
    }

    @Test
    void maxTreatsTwoNumbersAsDataNotAsARange() throws Exception {
        worker.assignTask(4, "MAX", new int[]{5, 3});

        assertEquals("5", awaitReportedResult(4),
                "a two element MAX payload is still just two numbers - reading it as a range 5..3 would "
                        + "produce an empty list and a nonsense answer");
    }
}
