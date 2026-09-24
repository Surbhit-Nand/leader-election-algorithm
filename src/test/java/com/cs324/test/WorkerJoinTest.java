package com.cs324.test;

import com.cs324.bootstrap.BootstrapRegistryImpl;
import com.cs324.common.WorkerRemote;
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the "a new worker joins through an existing active worker" handoff -
 * the part of the Bootstrap contract that WorkerNode relies on.
 *
 * These tests deliberately drive the same calls WorkerNode will make, so that
 * the worker-side startup code has a fixed contract to satisfy:
 *
 *   register with Bootstrap -> receive a live peer stub -> connect to it
 *
 * Uses its own port, separate from BootstrapRegistryImplTest, so the two test
 * classes never fight over the same registry.
 */
class WorkerJoinTest {

    private static final int TEST_RMI_PORT = 19998;
    private static final String HOST = "localhost";

    private static Registry registry;

    private BootstrapRegistryImpl bootstrap;

    @BeforeAll
    static void startRegistry() throws RemoteException {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @BeforeEach
    void setUp() throws RemoteException {
        bootstrap = new BootstrapRegistryImpl();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(bootstrap);
    }

    private void unexportQuietly(Remote remoteObject) {
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
            // already unexported, nothing to do
        }
    }

    @Test
    void aJoiningWorkerCanConnectToThePeerTheBootstrapHandedIt() throws Exception {
        // Worker 1 is already in the network: registered AND bound, so the
        // Bootstrap Node has something live to introduce to newcomers.
        FakeWorkerRemote worker1 = new FakeWorkerRemote(1);
        registry.rebind("Worker1", worker1);
        bootstrap.registerWorker(1, HOST, TEST_RMI_PORT);

        // Worker 2 joins and is handed an existing active worker.
        WorkerRemote peer = bootstrap.registerWorker(2, HOST, TEST_RMI_PORT);
        assertNotNull(peer, "a joining worker must be handed a peer, otherwise the network can never form");

        // This is exactly the call WorkerNode makes to establish the edge.
        // If the peer were bound under the wrong name, or not bound at all,
        // this is where the join would fail.
        assertDoesNotThrow(() -> peer.connectAsNeighbour(2, HOST, TEST_RMI_PORT),
                "the peer stub handed out by the Bootstrap must accept the neighbour connection over RMI");

        unexportQuietly(worker1);
    }

    @Test
    void registeringWhileTheChosenPeerIsNotBoundSkipsStalePeerAndKeepsNewWorker() throws Exception {
        // Worker 1 is in the Bootstrap's active list but was never bound in the
        // registry - e.g. it crashed, or bound under a different name.
        bootstrap.registerWorker(1, HOST, TEST_RMI_PORT);

        // Worker 2 registers: the stale peer is skipped (and pruned), the join
        // succeeds with no live peer, and the newcomer is still recorded.
        WorkerRemote peer = assertDoesNotThrow(() -> bootstrap.registerWorker(2, HOST, TEST_RMI_PORT),
                "a stale peer must be skipped, not fail the whole join");
        assertNull(peer, "with only a stale peer available there is no live peer to hand out");
        assertTrue(bootstrap.getActiveWorkerIds().contains(2),
                "fixed behaviour: the failed peer lookup must not drop worker 2 from the active list");
    }
}
