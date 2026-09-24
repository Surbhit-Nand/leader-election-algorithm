package com.cs324.worker;

import com.cs324.common.WorkerRemote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.NotBoundException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WorkerRemoteImpl implementing distributed leader election, echo algorithm, and concurrent task execution.
 */
public class WorkerRemoteImpl extends UnicastRemoteObject implements WorkerRemote {

    private static class ElectionState {
        int parentId;
        int bestCandidateId;
        int bestCandidateJac;
        final Set<Integer> pendingChildren = ConcurrentHashMap.newKeySet();
        boolean echoed = false;

        ElectionState(int parentId, int bestCandidateId, int bestCandidateJac) {
            this.parentId = parentId;
            this.bestCandidateId = bestCandidateId;
            this.bestCandidateJac = bestCandidateJac;
        }
    }

    private final int id;
    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private long electionCounter = 0; // Guaranteed unique incrementing counter for election IDs
    private volatile int currentCoordinatorId = -1; // -1 = no known coordinator yet
    private final Set<Long> seenMessageIds = ConcurrentHashMap.newKeySet();
    private final Set<Long> seenCoordinatorMessageIds = ConcurrentHashMap.newKeySet();
    private final Map<Long, ElectionState> elections = new ConcurrentHashMap<>();
    private final Map<Integer, WorkerRemote> neighbours = new ConcurrentHashMap<>();
    
    // Concurrency & Task tracking
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Map<Integer, Object> taskResults = new ConcurrentHashMap<>();

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
        return jobAllocationCounter.get();
    }

    public int getCurrentCoordinatorId() {
        return currentCoordinatorId;
    }

    @Override
    public boolean isActive() throws RemoteException {
        return true;
    }

    /**
     * Initiates a new election wave from this node using a bitwise-packed unique message ID.
     */
    public synchronized long startElection() {
        // High 32 bits = worker id, Low 32 bits = incrementing election counter
        long messageId = ((long) id << 32) | (electionCounter++);
        seenMessageIds.add(messageId);

        int currentJac = jobAllocationCounter.get();
        ElectionState election = new ElectionState(this.id, this.id, currentJac);

        // All neighbours are pending children initially
        for (Integer neighborId : neighbours.keySet()) {
            election.pendingChildren.add(neighborId);
        }

        elections.put(messageId, election);

        // Broadcast election message to all neighbours
        for (Map.Entry<Integer, WorkerRemote> entry : neighbours.entrySet()) {
            try {
                entry.getValue().receiveElectionMessage(this.id, this.id, currentJac, messageId);
            } catch (RemoteException e) {
                election.pendingChildren.remove(entry.getKey());
            }
        }

        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
        return messageId;
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        if (!seenMessageIds.add(messageId)) {
            // Duplicate ELECTION: reply immediately to prevent deadlock
            ElectionState existing = elections.get(messageId);
            if (existing != null) {
                try {
                    WorkerRemote sender = neighbours.get(originId);
                    if (sender != null) {
                        sender.receiveEchoMessage(messageId, this.id, existing.bestCandidateId, existing.bestCandidateJac);
                    }
                } catch (RemoteException ignored) {}
            }
            return;
        }

        int currentJac = jobAllocationCounter.get();
        int bestId = candidateId;
        int bestJac = candidateJac;

        if (currentJac < bestJac) {
            bestId = this.id;
            bestJac = currentJac;
        } else if (currentJac == bestJac) {
            if (this.id > bestId) {
                bestId = this.id;
                bestJac = currentJac;
            }
        }

        ElectionState election = new ElectionState(originId, bestId, bestJac);

        for (Integer neighborId : neighbours.keySet()) {
            if (neighborId != originId) {
                election.pendingChildren.add(neighborId);
            }
        }

        elections.put(messageId, election);

        for (Map.Entry<Integer, WorkerRemote> entry : neighbours.entrySet()) {
            if (entry.getKey() != originId) {
                try {
                    entry.getValue().receiveElectionMessage(this.id, bestId, bestJac, messageId);
                } catch (RemoteException e) {
                    election.pendingChildren.remove(entry.getKey());
                }
            }
        }

        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        if (!seenCoordinatorMessageIds.add(messageId)) {
            return;
        }
        this.currentCoordinatorId = coordinatorId;
        
        for (Map.Entry<Integer, WorkerRemote> entry : neighbours.entrySet()) {
            try {
                entry.getValue().receiveCoordinatorMessage(coordinatorId, messageId);
            } catch (RemoteException ignored) {}
        }
    }

    @Override
    public void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException {
        ElectionState election = elections.get(messageId);
        if (election == null) {
            return;
        }

        synchronized (election) {
            if (election.bestCandidateJac < bestCandidateJac) {
                election.bestCandidateId = bestCandidateId;
                election.bestCandidateJac = bestCandidateJac;
            } else if (election.bestCandidateJac == bestCandidateJac) {
                if (bestCandidateId > election.bestCandidateId) {
                    election.bestCandidateId = bestCandidateId;
                    election.bestCandidateJac = bestCandidateJac;
                }
            }

            election.pendingChildren.remove(fromWorkerId);
            
            if (election.pendingChildren.isEmpty() && !election.echoed) {
                propagateEcho(messageId, election);
            }
        }
    }

    private void propagateEcho(long messageId, ElectionState election) {
        synchronized (election) {
            if (election.echoed) {
                return;
            }
            election.echoed = true;
        }

        if (election.parentId == this.id) {
            this.currentCoordinatorId = election.bestCandidateId;
            long coordMessageId = messageId + 1000000L; // Separate coordinator message ID space
            for (Map.Entry<Integer, WorkerRemote> entry : neighbours.entrySet()) {
                try {
                    entry.getValue().receiveCoordinatorMessage(election.bestCandidateId, coordMessageId);
                } catch (RemoteException ignored) {}
            }
        } else {
            WorkerRemote parentStub = neighbours.get(election.parentId);
            if (parentStub != null) {
                try {
                    parentStub.receiveEchoMessage(messageId, this.id, election.bestCandidateId, election.bestCandidateJac);
                } catch (RemoteException ignored) {}
            }
        }
    }

    @Override
    public void connectAsNeighbour(int newWorker, String rmiHost, int rmiPort) throws RemoteException {
        try {
            Registry registry = LocateRegistry.getRegistry(rmiHost, rmiPort);
            WorkerRemote stub = (WorkerRemote) registry.lookup("Worker" + newWorker);
            neighbours.put(newWorker, stub);
        } catch (NotBoundException e) {
            throw new RemoteException("Failed to look up new neighbour", e);
        }
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        jobAllocationCounter.incrementAndGet();

        executor.submit(() -> {
            Object computedResult;
            try {
                if ("MAX".equalsIgnoreCase(jobType)) {
                    computedResult = computeMax(payload);
                } else if ("PRIMESUM".equalsIgnoreCase(jobType)) {
                    computedResult = computePrimeSum(payload);
                } else if ("PRIMECOUNT".equalsIgnoreCase(jobType)) {
                    computedResult = computePrimeCount(payload);
                } else {
                    computedResult = "Unsupported job type: " + jobType;
                }
            } catch (Exception e) {
                computedResult = "Error executing task: " + e.getMessage();
            }

            try {
                if (currentCoordinatorId == id || currentCoordinatorId == -1) {
                    reportTaskResult(jobId, id, computedResult);
                } else {
                    WorkerRemote coordinatorStub = neighbours.get(currentCoordinatorId);
                    if (coordinatorStub != null) {
                        coordinatorStub.reportTaskResult(jobId, id, computedResult);
                    } else {
                        reportTaskResult(jobId, id, computedResult);
                    }
                }
            } catch (RemoteException ignored) {}
        });
    }

    @Override
    public void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException {
        taskResults.put(jobId, result);
    }

    // --- Helper Computation Methods ---

    private int computeMax(Object payload) {
        int max = Integer.MIN_VALUE;
        for (int num : extractNumbers("MAX", payload)) {
            if (num > max) {
                max = num;
            }
        }
        return max == Integer.MIN_VALUE ? 0 : max;
    }

    private long computePrimeSum(Object payload) {
        long sum = 0;
        for (int num : extractNumbers("PRIMESUM", payload)) {
            if (isPrime(num)) {
                sum += num;
            }
        }
        return sum;
    }

    private int computePrimeCount(Object payload) {
        int count = 0;
        for (int num : extractNumbers("PRIMECOUNT", payload)) {
            if (isPrime(num)) {
                count++;
            }
        }
        return count;
    }

    private List<Integer> extractNumbers(String jobType, Object payload) {
        List<Integer> list = new ArrayList<>();
        if (payload instanceof int[]) {
            int[] arr = (int[]) payload;
            if (arr.length == 2 && !"MAX".equalsIgnoreCase(jobType)) {
                int start = arr[0];
                int end = arr[1];
                for (int i = start; i <= end; i++) {
                    list.add(i);
                }
            } else {
                for (int v : arr) {
                    list.add(v);
                }
            }
        } else if (payload instanceof Integer[]) {
            for (Integer v : (Integer[]) payload) {
                if (v != null) list.add(v);
            }
        } else if (payload instanceof Collection) {
            for (Object o : (Collection<?>) payload) {
                if (o instanceof Number) list.add(((Number) o).intValue());
            }
        }
        return list;
    }

    private boolean isPrime(int n) {
        if (n <= 1) return false;
        if (n <= 3) return true;
        if (n % 2 == 0 || n % 3 == 0) return false;
        for (int i = 5; i * i <= n; i += 6) {
            if (n % i == 0 || n % (i + 2) == 0) return false;
        }
        return true;
    }
}