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

    // Called by a neighbour to propagate the reply once a subtree has finished.
    void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException;

    void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException;

    // Called by a neighbour to propagate the COORDINATOR result.
    void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException;

    // Called by the coordinator to assign a portion of a job to this worker.
    void assignTask(int jobId, String jobType, Object payload) throws RemoteException;   // fire-and-return
    void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException; // callback when done
}
