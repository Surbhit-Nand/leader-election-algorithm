package com.cs324.bootstrap;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerRemote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keep the active-worker map thread-safe (ConcurrentHashMap is a good start)
 * since multiple workers may register/deregister concurrently.
 */
public class BootstrapRegistryImpl extends UnicastRemoteObject implements BootstrapRemote {

    private final Map<Integer, WorkerAddress> activeWorkers = new ConcurrentHashMap<>();

    private final java.util.concurrent.atomic.AtomicLong nextJobId =
            new java.util.concurrent.atomic.AtomicLong(1);

    @Override
    public int allocateJobId() throws RemoteException {
        // Allocate once for the whole cluster, rather than once per client window.
        long jobId = nextJobId.getAndIncrement();
        if (jobId > Integer.MAX_VALUE) throw new RemoteException("Job IDs exhausted.");
        return (int) jobId;
    }

    public BootstrapRegistryImpl() throws RemoteException {
        super();
    }

    @Override
    public WorkerRemote registerWorker(int workerId, String rmiHost, int rmiPort) throws RemoteException {
        // Copy workers before picking a peer, so the newcomer is never handed itself.
        List<Map.Entry<Integer, WorkerAddress>> snapshot = new ArrayList<>(activeWorkers.entrySet());
        // Randomise the order so the peer is random.
        Collections.shuffle(snapshot, new Random());

        // The peer we will hand back (null = first worker).
        WorkerRemote peerStub = null;
        // Try each candidate until one answers.
        for (Map.Entry<Integer, WorkerAddress> candidate : snapshot) {
            try {
                // Contact the candidate's registry.
                Registry peerRegistry = LocateRegistry.getRegistry(
                        candidate.getValue().getRmiHost(), candidate.getValue().getRmiPort());
                // Look up the peer worker.
                peerStub = (WorkerRemote) peerRegistry.lookup("Worker" + candidate.getKey());
                break; // first live peer wins
            } catch (NotBoundException e) {
                // Stale entry: prune it so it stops blocking future joins.
                // Remove only if unchanged (key+value match).
                activeWorkers.remove(candidate.getKey(), candidate.getValue());
            } catch (RemoteException e) {
                // Peer unreachable right now: try the next candidate.
            }
        }

        // Group 6: atomic insert. The old containsKey()+put() pair allowed two
        // concurrent registerWorker(sameId) calls to both succeed (lost update);
        // putIfAbsent makes duplicate detection atomic without changing semantics.
        // Insert only if the ID is free; returns the existing entry otherwise.
        WorkerAddress prev = activeWorkers.putIfAbsent(workerId, new WorkerAddress(rmiHost, rmiPort));
        // ID was already taken: reject.
        if (prev != null) {
            throw new RemoteException("Duplicate worker id " + workerId + " - already registered");
        }
        return peerStub;
    }

    @Override
    public void deregisterWorker(int workerId) throws RemoteException {
        activeWorkers.remove(workerId);
    }

    @Override
    public List<Integer> getActiveWorkerIds() throws RemoteException {
        return new ArrayList<>(activeWorkers.keySet());
        
    }

    @Override
    public WorkerAddress getWorkerAddress(int workerId) throws RemoteException {
        return activeWorkers.get(workerId);
    }
}
