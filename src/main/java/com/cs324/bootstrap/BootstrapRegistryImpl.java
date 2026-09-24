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
        if (activeWorkers.containsKey(workerId)) {
            throw new RemoteException("Duplicate worker id " + workerId + " - already registered");
        }

        // Shuffle so the peer is random; skip stale entries instead of failing
        // the whole join when one peer has crashed or was never bound.
        List<Map.Entry<Integer, WorkerAddress>> snapshot = new ArrayList<>(activeWorkers.entrySet());
        Collections.shuffle(snapshot, new Random());

        WorkerRemote peerStub = null;
        for (Map.Entry<Integer, WorkerAddress> candidate : snapshot) {
            try {
                Registry peerRegistry = LocateRegistry.getRegistry(
                        candidate.getValue().getRmiHost(), candidate.getValue().getRmiPort());
                peerStub = (WorkerRemote) peerRegistry.lookup("Worker" + candidate.getKey());
                break; // first live peer wins
            } catch (NotBoundException e) {
                // Stale entry: prune it so it stops blocking future joins.
                activeWorkers.remove(candidate.getKey());
            } catch (RemoteException e) {
                // Peer unreachable right now: try the next candidate.
            }
        }

        activeWorkers.put(workerId, new WorkerAddress(rmiHost, rmiPort));
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
