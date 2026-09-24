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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step 4 of the worker test plan: receiveEchoMessage() and the echo propagation
 * it drives.
 *
 * This is where the election actually terminates. A node forwards the ELECTION
 * outward and then has to wait for every one of its children to report back.
 * Only when the last child has answered does it pass the running best candidate
 * up to its parent - and the initiator, which has no parent, is the node that
 * finally declares the winner and announces it.
 *
 * Termination is therefore an exact, local condition ("every child replied"),
 * not a timeout. These tests pin that down, plus the once-only guard that stops
 * a repeated echo from being propagated twice.
 */
class WorkerEchoTest {

    private static final int TEST_RMI_PORT = 19803;
    private static final String HOST = "localhost";
    private static final long ELECTION_ID = 77L;

    private static Registry registry;

    private WorkerRemoteImpl worker;      // id 1 - the node under test
    private RecordingWorkerRemote parent; // id 2 - who the echo must be sent up to
    private RecordingWorkerRemote child;  // id 3 - who owes this node an echo

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
        worker = new WorkerRemoteImpl(1);
        parent = new RecordingWorkerRemote(2);
        child = new RecordingWorkerRemote(3);

        registry.rebind("Worker2", parent);
        registry.rebind("Worker3", child);

        worker.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        worker.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(worker);
        unexportQuietly(parent);
        unexportQuietly(child);
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

    private static long countOf(List<String> calls, String prefix) {
        return calls.stream().filter(call -> call.startsWith(prefix)).count();
    }

    private static boolean receivedCoordinatorFor(List<String> calls, int coordinatorId) {
        return calls.stream().anyMatch(call -> call.startsWith("coordinator|" + coordinatorId + "|"));
    }

    /** Puts this worker in the middle of an election: parent = 2, only child = 3. */
    private void receiveElectionFromParent() throws Exception {
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID);
    }

    @Test
    void theLastEchoIsForwardedUpToTheParent() throws Exception {
        receiveElectionFromParent();

        worker.receiveEchoMessage(ELECTION_ID, 3, 3, 0);

        assertTrue(parent.receivedEcho(1, ELECTION_ID),
                "with its only child now answered, the node must report the result to its parent; "
                        + "recorded: " + parent.calls());
    }

    @Test
    void aRepeatedEchoFromTheSameChildIsNotPropagatedTwice() throws Exception {
        receiveElectionFromParent();

        worker.receiveEchoMessage(ELECTION_ID, 3, 3, 0);
        worker.receiveEchoMessage(ELECTION_ID, 3, 3, 0); // same child answering again

        assertEquals(1L, countOf(parent.calls(), "echo|"),
                "a node must report upward exactly once; a duplicate echo must not trigger a second propagation; "
                        + "recorded: " + parent.calls());
    }

    @Test
    void theInitiatorAnnouncesTheWinnerOnceEveryChildHasEchoed() throws Exception {
        WorkerRemoteImpl initiator = new WorkerRemoteImpl(1);
        try {
            initiator.connectAsNeighbour(3, HOST, TEST_RMI_PORT);

            long electionId = initiator.startElection();
            initiator.receiveEchoMessage(electionId, 3, 3, 0);

            // Both candidates sit on JAC 0, so the tie-break decides it: higher id wins.
            assertEquals(3, initiator.getCurrentCoordinatorId(),
                    "both candidates have JAC 0, so the higher worker id must win the tie");

            assertTrue(receivedCoordinatorFor(child.calls(), 3),
                    "the initiator has no parent, so it must announce the winner to its neighbours; "
                            + "recorded: " + child.calls());
        } finally {
            unexportQuietly(initiator);
        }
    }
}
