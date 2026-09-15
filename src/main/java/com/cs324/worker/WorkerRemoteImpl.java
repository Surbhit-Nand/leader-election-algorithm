package com.cs324.worker;

import com.cs324.common.WorkerRemote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: implement WorkerRemote.
 *
 * Election message de-duplication: keep a thread-safe Set of seen messageIds
 * (e.g. Collections.synchronizedSet or ConcurrentHashMap.newKeySet()) and
 * drop any receiveElectionMessage/receiveCoordinatorMessage call whose
 * messageId has already been seen.
 *
 * Concurrency: executeTask should be safe to call from multiple threads at
 * once (the coordinator may hand this worker several sub-tasks belonging to
 * different jobs concurrently) - consider an ExecutorService per worker.
 */
public class WorkerRemoteImpl extends UnicastRemoteObject implements WorkerRemote {

    private final int id;
    private volatile int jobAllocationCounter = 0;
    private final Set<Long> seenMessageIds = ConcurrentHashMap.newKeySet();

    public WorkerRemoteImpl(int id) throws RemoteException {
        super();
        this.id = id;
    }

    @Override
    public int getId() throws RemoteException {
        return id;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        return jobAllocationCounter;
    }

    @Override
    public boolean isActive() throws RemoteException {
        return true; // TODO
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        // TODO: check seenMessageIds first, then compare JAC / ID and forward
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        // TODO: check seenMessageIds first, then record coordinator and forward
    }

    @Override
    public Object executeTask(String jobType, Object payload) throws RemoteException {
        // TODO: dispatch to MAX / PRIMESUM / PRIMECOUNT logic on a worker thread
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
