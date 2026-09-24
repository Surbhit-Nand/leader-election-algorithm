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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step 3 of the worker test plan: receiveElectionMessage().
 *
 * Two rules matter here, and they pull in opposite directions:
 *
 *   1. An ELECTION must reach every neighbour EXCEPT the one it arrived from,
 *      so it spreads outward instead of bouncing back to the parent.
 *   2. A message id that has already been processed must not be re-forwarded -
 *      otherwise a cycle in the unstructured network loops forever.
 *
 * ...but rule 2 cannot mean "silently drop it". The node that forwarded to us is
 * waiting for our reply, so a duplicate still has to be ANSWERED, or two nodes
 * that forwarded to each other simply wait forever. These tests pin down both
 * halves.
 */
class WorkerElectionMessageTest {

    private static final int TEST_RMI_PORT = 19802;
    private static final String HOST = "localhost";
    private static final long ELECTION_ID = 42L;

    private static Registry registry;

    private WorkerRemoteImpl worker;      // id 1 - the node under test
    private RecordingWorkerRemote sender; // id 2 - the neighbour the ELECTION arrives from
    private RecordingWorkerRemote other;  // id 3 - the neighbour it must be forwarded to

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
        sender = new RecordingWorkerRemote(2);
        other = new RecordingWorkerRemote(3);

        registry.rebind("Worker2", sender);
        registry.rebind("Worker3", other);

        worker.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        worker.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(worker);
        unexportQuietly(sender);
        unexportQuietly(other);
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

    @Test
    void anElectionIsForwardedToEveryNeighbourExceptTheSender() throws Exception {
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID);

        assertTrue(other.receivedElection(1, ELECTION_ID),
                "the election must be passed on to the other neighbours; recorded: " + other.calls());

        assertFalse(sender.receivedElection(1, ELECTION_ID),
                "the election must not be sent back to the neighbour it came from; recorded: " + sender.calls());
    }

    @Test
    void aDuplicateElectionIsNotForwardedTwiceButIsStillAnswered() throws Exception {
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID);
        worker.receiveElectionMessage(2, 2, 0, ELECTION_ID); // same election id again

        assertEquals(1L, countOf(other.calls(), "election|"),
                "a duplicate must not be forwarded a second time; recorded: " + other.calls());

        assertEquals(1L, countOf(sender.calls(), "echo|"),
                "a duplicate must still be answered with exactly one echo - dropping it silently leaves the "
                        + "forwarding node waiting forever, which deadlocks any network containing a cycle; "
                        + "recorded: " + sender.calls());
    }

    @Test
    void aWorkerWithNoOtherNeighboursEchoesStraightBack() throws Exception {
        // This worker's only neighbour is the one that sent the ELECTION, so it
        // has no children to wait for and must finish immediately.
        WorkerRemoteImpl leaf = new WorkerRemoteImpl(9);
        try {
            leaf.connectAsNeighbour(2, HOST, TEST_RMI_PORT);

            leaf.receiveElectionMessage(2, 2, 0, ELECTION_ID);

            assertEquals(1L, countOf(sender.calls(), "echo|"),
                    "a node with no children has already finished, so it must echo back immediately; "
                            + "recorded: " + sender.calls());
        } finally {
            unexportQuietly(leaf);
        }
    }
}
