package com.cs324.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI contract implemented by every Worker node.
 * Covers neighbour discovery, leader election, and job execution.
 */
public interface WorkerRemote extends Remote {

    int getId() throws RemoteException;

    int getJobAllocationCounter() throws RemoteException;

    boolean isActive() throws RemoteException;

    // Called by a neighbour to propagate an ELECTION message.
    // originId/messageId let the receiver detect and drop duplicates.
    void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException;

    void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException;

    // Called by a neighbour to propagate the COORDINATOR result.
    void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException;

    // Called by the coordinator to assign a portion of a job to this worker.
    Object executeTask(String jobType, Object payload) throws RemoteException;
}
