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
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Step 6 of the worker test plan: receiveCoordinatorMessage().
 *
 * Once the winner is known it must reach EVERY reachable worker, not just the
 * initiator's immediate neighbours - so a received COORDINATOR message is
 * relayed onward. Three things have to hold:
 *
 *   - the relay reaches workers more than one hop away,
 *   - a repeat of the same COORDINATOR message is not relayed again, otherwise
 *     the announcement keeps bouncing around the network, and
 *   - a LATER announcement is still processed, since a new election term has to
 *     replace the coordinator the network already believes in.
 */
class WorkerCoordinatorMessageTest {

    private static final int TEST_RMI_PORT = 19805;
    private static final String HOST = "localhost";
    private static final long MESSAGE_ID = 300L;
    private static final long SECOND_TERM_MESSAGE_ID = 301L;
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

    private static long countOf(List<String> calls, String prefix) {
        return calls.stream().filter(call -> call.startsWith(prefix)).count();
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

        assertEquals(1L, countOf(recorder.calls(), "coordinator|"),
                "a duplicate COORDINATOR message must not be relayed a second time, otherwise the announcement "
                        + "keeps circulating the network; recorded: " + recorder.calls());
    }

    @Test
    void aDifferentCoordinatorMessageIdIsProcessedAsANewAnnouncement() throws Exception {
        connect(one, 9);

        one.receiveCoordinatorMessage(3, MESSAGE_ID);

        assertEquals(3, one.getCurrentCoordinatorId(),
                "the announced coordinator must be adopted, so the network starts treating 3 as its leader");

        // A later term, so a different coordinator AND a different message id.
        one.receiveCoordinatorMessage(2, SECOND_TERM_MESSAGE_ID);

        assertEquals(2, one.getCurrentCoordinatorId(),
                "a later election must replace the coordinator the network already has - if the new term were "
                        + "treated as a duplicate of the old one, this worker would keep reporting 3 and keep "
                        + "sending jobs to a coordinator that no longer holds the role");

        assertTrue(recorder.receivedCoordinator(3, MESSAGE_ID),
                "the first announcement must be relayed on; recorded: " + recorder.calls());
        assertTrue(recorder.receivedCoordinator(2, SECOND_TERM_MESSAGE_ID),
                "the second announcement must be relayed on just like the first; recorded: " + recorder.calls());
        assertEquals(2L, countOf(recorder.calls(), "coordinator|"),
                "each term is announced once - the second is new work, not a repeat of the first; recorded: "
                        + recorder.calls());
    }

    @Test
    void coordinatorMessageConvergesAcrossACyclicTopology() throws Exception {
        // A triangle: 1-2, 2-3, 3-1. Unlike the line above, this topology
        // contains a cycle, so the announcement can come back to a node that has
        // already sent it. That only terminates because a COORDINATOR message
        // is relayed once and then suppressed everywhere it has been seen.
        connect(one, 2);
        connect(two, 1);
        connect(two, 3);
        connect(three, 2);
        connect(three, 1);
        connect(one, 3);

        // The call has to be bounded as well as the wait below: an unsuppressed
        // relay in a cycle means this call never returns, so it runs on its own
        // thread and fails the test instead of hanging the whole run.
        assertTimeoutPreemptively(Duration.ofMillis(AWAIT_TIMEOUT_MS), () ->
                one.receiveCoordinatorMessage(3, MESSAGE_ID));

        awaitCoordinatorOnAll(3);
    }
}
