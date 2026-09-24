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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BootstrapRegistryImplTest {

    // Fixed test port. If your test environment already has something bound
    // here, change this to a free port on your machine.
    private static final int TEST_RMI_PORT = 19999;

    // Created once for the whole test class, not per test. Recreating an RMI
    // registry on the same port between individual tests is flaky - the OS
    // doesn't always release the port instantly after unexporting, causing
    // "Address already in use" on the very next test.
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
        // Unbind anything this test registered, so names don't leak into
        // the next test that shares the same registry.
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
    void firstWorkerToRegisterGetsNullPeer() throws RemoteException {
        WorkerRemote peer = bootstrap.registerWorker(1, "localhost", TEST_RMI_PORT);

        assertNull(peer, "the very first worker to register should have no peer to connect to");
        assertEquals(1, bootstrap.getActiveWorkerIds().size(), "the first worker should still be added to the active list");
    }

    @Test
    void secondWorkerToRegisterGetsFirstWorkerAsPeer() throws Exception {
        // Worker 1 registers first and binds itself for real, so the
        // Bootstrap Node has something live to look up later.
        FakeWorkerRemote worker1 = new FakeWorkerRemote(1);
        registry.rebind("Worker1", worker1);
        bootstrap.registerWorker(1, "localhost", TEST_RMI_PORT);

        // Worker 2 registers second - it should be handed back worker 1.
        WorkerRemote peer = bootstrap.registerWorker(2, "localhost", TEST_RMI_PORT);

        assertNotNull(peer, "the second worker should be given an existing peer to connect to");
        assertEquals(1, peer.getId(), "worker 2's only possible peer is worker 1");
        assertEquals(2, bootstrap.getActiveWorkerIds().size(), "both workers should now be in the active list");

        unexportQuietly(worker1);
    }

        @Test
    void getActiveWorkerIdsIsEmptyWhenNoneRegistered() throws RemoteException {
        assertTrue(bootstrap.getActiveWorkerIds().isEmpty(), "a fresh Bootstrap Node should have no active workers yet");
    }
 
    @Test
    void getActiveWorkerIdsReflectsAllRegisteredWorkers() throws Exception {
        FakeWorkerRemote worker1 = new FakeWorkerRemote(1);
        registry.rebind("Worker1", worker1);
        bootstrap.registerWorker(1, "localhost", TEST_RMI_PORT);
        bootstrap.registerWorker(2, "localhost", TEST_RMI_PORT);
 
        List<Integer> activeIds = bootstrap.getActiveWorkerIds();
 
        assertEquals(2, activeIds.size(), "both registered workers should be reflected");
        assertTrue(activeIds.contains(1), "worker 1 should be in the active list");
        assertTrue(activeIds.contains(2), "worker 2 should be in the active list");
 
        unexportQuietly(worker1);
    }
 
    @Test
    void getActiveWorkerIdsExcludesDeregisteredWorkers() throws Exception {
        FakeWorkerRemote worker1 = new FakeWorkerRemote(1);
        registry.rebind("Worker1", worker1);
        bootstrap.registerWorker(1, "localhost", TEST_RMI_PORT);
        bootstrap.registerWorker(2, "localhost", TEST_RMI_PORT);
 
        bootstrap.deregisterWorker(1);
        List<Integer> activeIds = bootstrap.getActiveWorkerIds();
 
        assertEquals(1, activeIds.size(), "only the non-deregistered worker should remain");
        assertTrue(activeIds.contains(2), "worker 2 should still be active");
 
        unexportQuietly(worker1);
    }

    @Test
    void deregisterWorkerRemovesAnExistingWorker() throws RemoteException {
        bootstrap.registerWorker(1, "localhost", TEST_RMI_PORT);
        assertEquals(1, bootstrap.getActiveWorkerIds().size(), "worker should be registered before deregistering it");
 
        bootstrap.deregisterWorker(1);
 
        assertTrue(bootstrap.getActiveWorkerIds().isEmpty(), "the worker should no longer be active after deregistering");
    }
 
    @Test
    void deregisteringAnUnknownWorkerDoesNotThrow() {
        // 42 was never registered. Map.remove() on a missing key is a no-op,
        // so this should complete quietly rather than error out.
        assertDoesNotThrow(() -> bootstrap.deregisterWorker(42),
                "deregistering an unknown worker id should not throw");
    }


}