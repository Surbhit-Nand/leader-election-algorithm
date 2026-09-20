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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: implement BootstrapRemote.
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
        List<Map.Entry<Integer, WorkerAddress>> snapshot = new ArrayList<>(activeWorkers.entrySet());

        WorkerRemote peerStub = null;
        Map.Entry<Integer, WorkerAddress> chosen = null;
        if (!snapshot.isEmpty()) {
            Random random = new Random();
            chosen = snapshot.get(random.nextInt(snapshot.size()));
        }
        
        if(chosen != null){
            try {
                Registry peerRegistry = LocateRegistry.getRegistry(chosen.getValue().getRmiHost(), chosen.getValue().getRmiPort());
                peerStub = (WorkerRemote) peerRegistry.lookup("Worker" + chosen.getKey());
                
            } catch (NotBoundException e) {
                throw new RemoteException("Failed to look up peer worker", e);
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
}
