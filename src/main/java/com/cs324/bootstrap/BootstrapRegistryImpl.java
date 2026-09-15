package com.cs324.bootstrap;

import com.cs324.common.BootstrapRemote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: implement BootstrapRemote.
 * Keep the active-worker map thread-safe (ConcurrentHashMap is a good start)
 * since multiple workers may register/deregister concurrently.
 */
public class BootstrapRegistryImpl extends UnicastRemoteObject implements BootstrapRemote {

    private final Map<Integer, String> activeWorkers = new ConcurrentHashMap<>();

    public BootstrapRegistryImpl() throws RemoteException {
        super();
    }

    @Override
    public Integer registerWorker(int workerId, String rmiHost, int rmiPort) throws RemoteException {
        // TODO: pick a random existing active worker to return, then add this one
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void deregisterWorker(int workerId) throws RemoteException {
        // TODO
    }

    @Override
    public List<Integer> getActiveWorkerIds() throws RemoteException {
        // TODO
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
