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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Step 6 of the worker test plan: receiveCoordinatorMessage().
 *
 * Once the winner is known it must reach EVERY reachable worker, not just the
 * initiator's immediate neighbours - so a received COORDINATOR message is
 * relayed onward. Two things have to hold:
 *
 *   - the relay reaches workers more than one hop away, and
 *   - a repeat of the same COORDINATOR message is not relayed again, otherwise
 *     the announcement keeps bouncing around the network.
 */
class WorkerCoordinatorMessageTest {

    private static final int TEST_RMI_PORT = 19805;
    private static final String HOST = "localhost";
    private static final long MESSAGE_ID = 300L;
    private static final long AWAIT_TIMEOUT_MS = 5_000;

    private static Registry registry;

    private WorkerRemoteImpl one;
    private WorkerRemoteImpl two;
    private WorkerRemoteImpl three;
    private RecordingWorkerRemote recorder;

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
        one = new WorkerRemoteImpl(1);
        two = new WorkerRemoteImpl(2);
        three = new WorkerRemoteImpl(3);
        recorder = new RecordingWorkerRemote(9);

        registry.rebind("Worker1", one);
        registry.rebind("Worker2", two);
        registry.rebind("Worker3", three);
        registry.rebind("Worker9", recorder);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(one);
        unexportQuietly(two);
        unexportQuietly(three);
        unexportQuietly(recorder);
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

    private void connect(WorkerRemoteImpl from, int toId) throws Exception {
        from.connectAsNeighbour(toId, HOST, TEST_RMI_PORT);
    }

    private void awaitCoordinatorOnAll(int expectedCoordinatorId) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            if (one.getCurrentCoordinatorId() == expectedCoordinatorId
                    && two.getCurrentCoordinatorId() == expectedCoordinatorId
                    && three.getCurrentCoordinatorId() == expectedCoordinatorId) {
                return;
            }
            Thread.sleep(20);
        }

        fail("the COORDINATOR message did not reach every worker within " + AWAIT_TIMEOUT_MS + "ms. Got: one="
                + one.getCurrentCoordinatorId() + ", two=" + two.getCurrentCoordinatorId()
                + ", three=" + three.getCurrentCoordinatorId());
    }

    @Test
    void theCoordinatorMessageReachesWorkersMoreThanOneHopAway() throws Exception {
        // A line: 1 - 2 - 3. Worker 3 sits two hops from where the announcement starts.
        connect(one, 2);
        connect(two, 1);
        connect(two, 3);
        connect(three, 2);

        one.receiveCoordinatorMessage(5, MESSAGE_ID);

        awaitCoordinatorOnAll(5);
    }

    @Test
    void aRepeatedCoordinatorMessageIsNotRelayedAgain() throws Exception {
        connect(one, 9);

        one.receiveCoordinatorMessage(7, MESSAGE_ID);
        one.receiveCoordinatorMessage(7, MESSAGE_ID); // same message id again

        long relays = recorder.calls().stream().filter(call -> call.startsWith("coordinator|")).count();
        assertEquals(1L, relays,
                "a duplicate COORDINATOR message must not be relayed a second time, otherwise the announcement "
                        + "keeps circulating the network; recorded: " + recorder.calls());
    }
}
