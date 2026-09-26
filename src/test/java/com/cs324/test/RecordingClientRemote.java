package com.cs324.test;

import com.cs324.common.ClientRemote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test double for ClientRemote: records every result the coordinator delivers,
 * so tests can assert on the aggregated values.
 */
public class RecordingClientRemote extends UnicastRemoteObject implements ClientRemote {

    private final Map<Integer, Object> results = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Integer> callOrder = new ConcurrentLinkedQueue<>();

    public RecordingClientRemote() throws RemoteException {
        super();
    }

    @Override
    public void receiveJobResult(int jobId, Object result) throws RemoteException {
        results.put(jobId, result);
        callOrder.add(jobId);
    }

    public Object resultFor(int jobId) {
        return results.get(jobId);
    }

    public boolean received(int jobId) {
        return results.containsKey(jobId);
    }

    public int callCount() {
        return callOrder.size();
    }
}