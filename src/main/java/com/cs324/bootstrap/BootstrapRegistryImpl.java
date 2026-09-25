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

    public BootstrapRegistryImpl() throws RemoteException {
        super();
    }

    @Override
    public WorkerRemote registerWorker(int workerId, String rmiHost, int rmiPort) throws RemoteException {
        // Copy workers before picking a peer, so the newcomer is never handed itself. If removed, a worker could be told to connect to itself.
        List<Map.Entry<Integer, WorkerAddress>> snapshot = new ArrayList<>(activeWorkers.entrySet());
        // Randomise the order so the peer is random. If removed, every newcomer joins via the same peer and it becomes a bottleneck.
        Collections.shuffle(snapshot, new Random());

        // The peer we will hand back (null = first worker). If removed, there is nowhere to store the chosen peer.
        WorkerRemote peerStub = null;
        // Try each candidate until one answers. If removed, joining fails whenever the first candidate is dead.
        for (Map.Entry<Integer, WorkerAddress> candidate : snapshot) {
            try {
                // Contact the candidate's registry. If removed, we can never find any peer.
                Registry peerRegistry = LocateRegistry.getRegistry(
                        candidate.getValue().getRmiHost(), candidate.getValue().getRmiPort());
                // Look up the peer worker. If removed, we hand back nothing and the newcomer stays isolated.
                peerStub = (WorkerRemote) peerRegistry.lookup("Worker" + candidate.getKey());
                break; // first live peer wins
            } catch (NotBoundException e) {
                // Stale entry: prune it so it stops blocking future joins.
                // Remove only if unchanged (key+value match). If removed, dead entries pile up and every join scans garbage.
                activeWorkers.remove(candidate.getKey(), candidate.getValue());
            } catch (RemoteException e) {
                // Peer unreachable right now: try the next candidate.
            }
        }

        // Group 6: atomic insert. The old containsKey()+put() pair allowed two
        // concurrent registerWorker(sameId) calls to both succeed (lost update);
        // putIfAbsent makes duplicate detection atomic without changing semantics.
        // Insert only if the ID is free; returns the existing entry otherwise. If removed (plain put), two workers with the same ID both succeed and the cluster breaks.
        WorkerAddress prev = activeWorkers.putIfAbsent(workerId, new WorkerAddress(rmiHost, rmiPort));
        // ID was already taken: reject. If removed, duplicates are silently accepted.
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
