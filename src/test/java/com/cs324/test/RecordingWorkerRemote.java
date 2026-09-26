package com.cs324.test;

import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test double that records every remote call it receives.
 *
 * Unlike FakeWorkerRemote (a silent no-op), this one keeps an audit trail, which
 * is what makes questions like "did the neighbour really receive that message
 * over RMI?" assertable instead of assumed.
 *
 * Every interface method is implemented, even the ones a given test does not
 * use, so this can be bound in a real RMI registry and looked up by name.
 */
public class RecordingWorkerRemote extends UnicastRemoteObject implements WorkerRemote {

    private final int id;
    private final List<String> calls = new CopyOnWriteArrayList<>();

    public RecordingWorkerRemote(int id) throws RemoteException {
        super();
        this.id = id;
    }

    // --- observation helpers used by tests ---

    public List<String> calls() {
        return List.copyOf(calls);
    }

    public boolean receivedElection(int originId, long messageId) {
        return calls.contains("election|" + originId + "|" + messageId);
    }

    public boolean receivedEcho(int fromWorkerId, long messageId) {
        return calls.contains("echo|" + fromWorkerId + "|" + messageId);
    }

    public boolean receivedCoordinator(int coordinatorId, long messageId) {
        return calls.contains("coordinator|" + coordinatorId + "|" + messageId);
    }

    public boolean receivedAssignment(int jobId, String jobType) {
        return calls.contains("assign|" + jobId + "|" + jobType);
    }

    public boolean receivedResult(int jobId, int workerId) {
        return calls.stream().anyMatch(call -> call.startsWith("result|" + jobId + "|" + workerId + "|"));
    }

    /** The value most recently reported for a job by a given worker, or null. */
    public String lastReportedResult(int jobId, int workerId) {
        String prefix = "result|" + jobId + "|" + workerId + "|";
        String last = null;
        for (String call : calls) {
            if (call.startsWith(prefix)) {
                last = call.substring(prefix.length());
            }
        }
        return last;
    }

    // --- WorkerRemote contract ---

    @Override
    public int getId() throws RemoteException {
        return id;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        return 0;
    }

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return id;
    }

    @Override
    public long startElection() throws RemoteException {
        calls.add("startElection");
        return 0L;
    }

    @Override
    public void ping() throws RemoteException {
        calls.add("ping");
    }

    @Override
    public void submitJob(int jobId, String jobType, Object payload, String clientHost, int clientPort, String clientName) throws RemoteException {
        calls.add("submit|" + jobId + "|" + jobType);
    }

    @Override
    public boolean isActive() throws RemoteException {
        return true;
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        calls.add("election|" + originId + "|" + messageId);
    }

    @Override
    public void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException {
        calls.add("echo|" + fromWorkerId + "|" + messageId);
    }

    @Override
    public void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException {
        calls.add("connect|" + newWorker + "|" + rmiHost + "|" + rmiPort);
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        calls.add("coordinator|" + coordinatorId + "|" + messageId);
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        calls.add("assign|" + jobId + "|" + jobType);
    }

    @Override
    public void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException {
        calls.add("result|" + jobId + "|" + workerId + "|" + result);
    }
}
