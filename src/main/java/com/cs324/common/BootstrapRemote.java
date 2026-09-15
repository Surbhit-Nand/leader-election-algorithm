package com.cs324.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * RMI contract for the Bootstrap Node.
 * Tracks registered active workers only - does not join elections or jobs.
 */
public interface BootstrapRemote extends Remote {

    // A worker registers itself and is returned an existing active worker
    // to randomly connect to (or null if it is the first node).
    Integer registerWorker(int workerId, String rmiHost, int rmiPort) throws RemoteException;

    void deregisterWorker(int workerId) throws RemoteException;

    List<Integer> getActiveWorkerIds() throws RemoteException;
}
