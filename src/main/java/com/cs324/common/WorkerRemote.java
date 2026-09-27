package com.cs324.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

// RMI contract every Worker node implements: discovery, election, and job execution.
public interface WorkerRemote extends Remote {

    // Identity, and the tie-break used when two workers share a JAC.
    int getId() throws RemoteException;

    // Work shed to other workers; the metric candidates are compared on.
    int getJobAllocationCounter() throws RemoteException;

    // Liveness probe target: a JVM that stops answering counts as failed.
    boolean isActive() throws RemoteException;

    // Current known coordinator id (-1 if none has been elected yet).
    int getCurrentCoordinatorId() throws RemoteException;

    // Starts a new election wave; returns the unique message id used.
    long startElection() throws RemoteException;

    // Heartbeat probe: succeeds as long as this worker's JVM is responsive.
    void ping() throws RemoteException;

    // Client entry point: the coordinator splits the payload, then calls the client back.
    // Called by a Client: the current coordinator splits the payload among
    // itself and its neighbours, then reports the aggregated result back to
    void submitJob(int jobId, String jobType, Object payload,
                   String clientHost, int clientPort, String clientName) throws RemoteException;

    // Neighbour-to-neighbour election wave; originId and messageId expose duplicates.
    // Called by a neighbour to propagate an ELECTION message.
    void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException;

    // Neighbour-to-neighbour reply, sent once a subtree has finished the wave.
    // Called by a neighbour to propagate the reply once a subtree has finished.
    void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException;

    // Adds a peer; both sides call it, so the neighbour edge ends up bidirectional.
    void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException;

    // The winning candidate's verdict, relayed outward by every node that hears it.
    // Called by a neighbour to propagate the COORDINATOR result.
    void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException;

    // Coordinator hands out one slice; the worker replies through reportTaskResult.
    // Called by the coordinator to assign a portion of a job to this worker.
    void assignTask(int jobId, String jobType, Object payload) throws RemoteException;   // fire-and-return
    void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException; // callback when done
}
