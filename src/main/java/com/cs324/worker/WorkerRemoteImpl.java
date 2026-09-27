package com.cs324.worker;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.TaskAssignment;
import com.cs324.common.ClientRemote;
import com.cs324.common.Constants;
import com.cs324.common.WorkerAddress;
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
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WorkerRemoteImpl implementing distributed leader election, echo algorithm, and concurrent task execution.
 */
public class WorkerRemoteImpl extends UnicastRemoteObject implements WorkerRemote {

    /** Required by the assignment spec: worker nodes hold a "cs324" marker. */
    public static final String leaderman = "cs324";

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
    // Holds the Job Allocation Counter. AtomicInteger = thread-safe counter.
    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    // Group 6: lock-free unique election IDs. Previously a plain long guarded by
    // synchronized startElection(); AtomicLong keeps IDs unique under concurrent
    // startElection() calls without serialising callers. Field name/type of
    // jobAllocationCounter is pinned by tests via reflection - do not rename it.
    // AtomicLong = thread-safe counter.
    private final AtomicLong electionCounter = new AtomicLong(0);
    // The currently known coordinator. volatile = every thread always sees the newest value.
    private volatile int currentCoordinatorId = -1; // -1 = no known coordinator yet
    // IDs of election messages already seen. Concurrent set = thread-safe.
    private final Set<Long> seenMessageIds = ConcurrentHashMap.newKeySet();
    // IDs of coordinator messages already seen.
    private final Set<Long> seenCoordinatorMessageIds = ConcurrentHashMap.newKeySet();
    // Ongoing elections by message ID. ConcurrentHashMap = safe for RMI threads.
    private final Map<Long, ElectionState> elections = new ConcurrentHashMap<>();
    // Known neighbours by worker ID. ConcurrentHashMap = safe to read while another thread adds a neighbour.
    private final Map<Integer, WorkerRemote> neighbours = new ConcurrentHashMap<>();

    /** Composite key so concurrent results for the SAME jobId from DIFFERENT workers never clobber each other. */
    // Record = tiny container for (jobId, workerId).
    private record JobResultKey(int jobId, int workerId) implements java.io.Serializable {}

    // Concurrency & Task tracking (Group 6)
    // Bounded pool with named daemon threads: concurrent jobs run in parallel,
    // a slow/long job never blocks unrelated jobs, and the pool can be shut down
    // cleanly via shutdown(). Daemon threads so a forgotten shutdown never hangs the JVM.
    // FixedThreadPool = limited workers running jobs in parallel.
    private final ExecutorService executor = Executors.newFixedThreadPool(
            // Pool size = CPUs x 2 (min 4). Enough parallelism without overload.
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
            // ThreadFactory = names each thread.
            new ThreadFactory() {
                // Counter for thread names. AtomicInteger = safe numbering.
                private final AtomicInteger seq = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    // Build one pool thread.
                    Thread t = new Thread(r, "worker-task-" + seq.incrementAndGet());
                    // Daemon = JVM can exit even if threads are alive.
                    t.setDaemon(true);
                    return t;
                }
            });
    // Per (jobId, workerId) results. ConcurrentHashMap gives lock-free thread-safe put/read.
    private final Map<JobResultKey, Object> taskResults = new ConcurrentHashMap<>();
    // Exactly-once guard: jobIds already accepted (in-flight or done). add() is atomic,
    // so concurrent duplicate assignTask(jobId) calls collapse to a single execution.
    // Job IDs are assumed unique per job; a repeated jobId is treated as a retry and suppressed.
    private final Set<Integer> seenJobIds = ConcurrentHashMap.newKeySet();
    // In-flight tasks for lifecycle observability (not used for locking).
    private final Set<Integer> activeTasks = ConcurrentHashMap.newKeySet();

    // ---- Coordinator job dispatch (client jobs) ----
    // A submitted job is split across the coordinator and its neighbours; the
    // coordinator tracks which workers it is still waiting on and aggregates
    // results once all have reported back.
    private record JobState(String jobType, Set<Integer> expected, String clientHost, int clientPort, String clientName, AtomicBoolean dispatchComplete) {}

    private final Set<Integer> submittedJobIds = ConcurrentHashMap.newKeySet();

    private final Map<Integer, JobState> coordinatingJobs = new ConcurrentHashMap<>();

    // ---- Heartbeat / coordinator-failure detection ----
    // Daemon scheduler so a forgotten shutdown never hangs the JVM.
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "worker-heartbeat");
        t.setDaemon(true);
        return t;
    });
    // Set by WorkerNode once the worker has registered with the bootstrap.
    private volatile BootstrapRemote bootstrap;
    // Cached coordinator stub (reused across beats; reset when it fails).
    private volatile WorkerRemote coordinatorStub;
    private volatile int cachedCoordinatorId = -1;
    // Skip a beat if the previous one is still blocked on a slow coordinator,
    // so a slow peer cannot stack up probes.
    private final AtomicBoolean heartbeatInFlight = new AtomicBoolean(false);
    // Number of client jobs this worker has coordinated in its current term.
    // A term lasts Constants.JOBS_PER_TERM client jobs, counted per job rather
    // than per assignment, so a term's length does not depend on how many
    // workers the cluster has. Reset whenever this node becomes the coordinator.
    private final AtomicInteger termJobsCoordinated = new AtomicInteger(0);

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

    @Override
    public int getCurrentCoordinatorId() throws RemoteException {
        return currentCoordinatorId;
    }

    /** Local helper for the WorkerNode console menu (not part of the RMI contract). */
    public Set<Integer> getNeighbourIds() {
        return Set.copyOf(neighbours.keySet());
    }

    /** Client jobs coordinated in the current term; a term ends at JOBS_PER_TERM. */
    public int getJobsCoordinatedInTerm() {
        return termJobsCoordinated.get();
    }

    @Override
    public boolean isActive() throws RemoteException {
        return true;
    }

    /**
     * Initiates a new election wave from this node using a bitwise-packed unique message ID.
     * Group 6: lock-free (no method-level synchronized). The ID is unique via AtomicLong;
     * per-election setup is thread-confined until published into the concurrent map,
     * and neighbour fan-out uses a snapshot so concurrent connectAsNeighbour() calls
     * cannot interfere with this wave.
     */
    public long startElection() {
        // Build a unique message ID from worker ID + counter. AtomicLong = no two elections share an ID.
        long messageId = ((long) id << 32) | (electionCounter.getAndIncrement());
        // Remember this ID so repeats are recognised as duplicates.
        seenMessageIds.add(messageId);

        // Read our JAC for the candidate comparison. Atomic get = always the newest value.
        int currentJac = jobAllocationCounter.get();
        // Create this wave's tracking object (starts with us as best candidate).
        ElectionState election = new ElectionState(this.id, this.id, currentJac);

        // Expect a reply from every current neighbour.
        for (Integer neighborId : neighbours.keySet()) {
            election.pendingChildren.add(neighborId);
        }

        // Publish the wave so echo replies can find it.
        elections.put(messageId, election);

        // Snapshot: blocking RMI calls run against a stable view; failures only
        // prune this wave's pendingChildren, never the shared neighbour map.
        // Copy of neighbours.
        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        // Send the election message to every neighbour.
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            try {
                entry.getValue().receiveElectionMessage(this.id, this.id, currentJac, messageId);
            } catch (RemoteException e) {
                // Peer is dead: stop waiting for it.
                election.pendingChildren.remove(entry.getKey());
            }
        }

        // No neighbours (or all dead) = wave is done, send our echo up immediately.
        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
        return messageId;
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        // Record the id as seen. This set is a record only: the duplicate guard
        // is the elections.compute() below, NOT this set.
        seenMessageIds.add(messageId);

        // Read our JAC for comparison.
        int currentJac = jobAllocationCounter.get();
        // Start assuming the incoming candidate is best.
        int bestId = candidateId;
        int bestJac = candidateJac;

        // Lower JAC wins (less-loaded worker leads).
        if (currentJac < bestJac) {
            bestId = this.id;
            bestJac = currentJac;
        } else if (currentJac == bestJac) {
            // Tie: higher worker ID wins.
            if (this.id > bestId) {
                bestId = this.id;
                bestJac = currentJac;
            }
        }

        // The comparison above reassigns these, so the lambda needs its own copy.
        final int ourBestId = bestId;
        final int ourBestJac = bestJac;
        final int parentId = originId;

        // Claim the wave and publish its state in one atomic map operation.
        // A separate put() left a gap in which a duplicate, or an echo from an
        // already-forwarded child, saw the id as seen but found no state and was
        // dropped - and a dropped echo strands that child in the parent's
        // pendingChildren forever, deadlocking a wave that contains a cycle.
        // compute() closes the gap, and running it before any fan-out means no
        // echo can arrive before there is a state to merge into.
        final ElectionState[] created = new ElectionState[1];
        final ElectionState[] alreadySeen = new ElectionState[1];
        elections.compute(messageId, (key, existing) -> {
            if (existing != null) {
                alreadySeen[0] = existing;
                return existing;
            }
            // Track this wave with us remembering who sent it (parent).
            ElectionState election = new ElectionState(parentId, ourBestId, ourBestJac);
            // Expect replies from all neighbours except the parent.
            for (Integer neighborId : neighbours.keySet()) {
                if (neighborId != parentId) {
                    election.pendingChildren.add(neighborId);
                }
            }
            created[0] = election;
            return election;
        });

        ElectionState seen = alreadySeen[0];
        if (seen != null) {
            // Already seen: answer the duplicate with our current best so the
            // sender can finish. Read under the monitor, because that is where
            // receiveEchoMessage merges candidates into it.
            int[] best = new int[2];
            synchronized (seen) {
                best[0] = seen.bestCandidateId;
                best[1] = seen.bestCandidateJac;
            }
            try {
                // Find who sent it.
                WorkerRemote sender = neighbours.get(parentId);
                if (sender != null) {
                    // Answer the duplicate with our current best so the sender can finish.
                    sender.receiveEchoMessage(messageId, this.id, best[0], best[1]);
                }
            } catch (RemoteException ignored) {}
            return;
        }

        ElectionState election = created[0];

        // Snapshot so concurrent neighbour changes mid-wave cannot corrupt fan-out.
        // Copy of neighbours.
        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        // Forward to every neighbour except the parent.
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            if (entry.getKey() != originId) {
                try {
                    entry.getValue().receiveElectionMessage(this.id, ourBestId, ourBestJac, messageId);
                } catch (RemoteException e) {
                    // Dead peer: stop waiting for it.
                    election.pendingChildren.remove(entry.getKey());
                }
            }
        }

        // No children = leaf node, echo back immediately.
        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        // Drop repeats of the same announcement.
        if (!seenCoordinatorMessageIds.add(messageId)) {
            return;
        }
        // Record the winner. Volatile write = all threads see it at once.
        this.currentCoordinatorId = coordinatorId;
        // A new term begins whenever this node is named coordinator, so the
        // job budget restarts rather than carrying over from an earlier term.
        if (coordinatorId == this.id) {
            termJobsCoordinated.set(0);
        }

        // Snapshot: relay runs on the calling RMI thread against a stable view.
        // Copy of neighbours.
        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        // Relay the winner to every neighbour so it reaches all hops.
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            try {
                entry.getValue().receiveCoordinatorMessage(coordinatorId, messageId);
            } catch (RemoteException ignored) {}
        }
    }

    @Override
    public void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException {
        // Find the matching wave.
        ElectionState election = elections.get(messageId);
        // Unknown wave (late/old echo): ignore.
        if (election == null) {
            return;
        }

        // Lock this wave so two echoes arriving together cannot corrupt the merge.
        synchronized (election) {
            // Keep the lower JAC.
            if (bestCandidateJac < election.bestCandidateJac) {
                election.bestCandidateId = bestCandidateId;
                election.bestCandidateJac = bestCandidateJac;
            } else if (election.bestCandidateJac == bestCandidateJac) {
                // Tie: keep the higher ID.
                if (bestCandidateId > election.bestCandidateId) {
                    election.bestCandidateId = bestCandidateId;
                    election.bestCandidateJac = bestCandidateJac;
                }
            }

            // One child answered, stop waiting for it.
            election.pendingChildren.remove(fromWorkerId);

            // All children answered and we haven't echoed yet: echo upward.
            if (election.pendingChildren.isEmpty() && !election.echoed) {
                propagateEcho(messageId, election);
            }
        }
    }

    private void propagateEcho(long messageId, ElectionState election) {
        // Lock + claim the echo so it is sent exactly once.
        synchronized (election) {
            if (election.echoed) {
                return;
            }
            election.echoed = true;
        }

        // We started this wave: we now know the winner.
        if (election.parentId == this.id) {
            // Save the winner locally.
            this.currentCoordinatorId = election.bestCandidateId;
            // Re-electing ourselves (a lone worker, or one whose peers all left)
            // is a fresh term, so the job budget restarts.
            if (election.bestCandidateId == this.id) {
                termJobsCoordinated.set(0);
            }
            // Coordinator announcement ID derived from the election ID.
            long coordMessageId = messageId + 1000000L;
            // Snapshot of neighbours for the broadcast.
            List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
            // Announce the winner to all neighbours.
            for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
                try {
                    entry.getValue().receiveCoordinatorMessage(election.bestCandidateId, coordMessageId);
                } catch (RemoteException ignored) {}
            }
        } else {
            // Forward our best upward to our parent.
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
            // Contact the new neighbour's registry.
            Registry registry = LocateRegistry.getRegistry(rmiHost, rmiPort);
            // Look up its worker object.
            WorkerRemote stub = (WorkerRemote) registry.lookup("Worker" + newWorker);
            // Save it (overwrites any old entry for that ID).
            neighbours.put(newWorker, stub);
        } catch (NotBoundException e) {
            // Name not found: report failure.
            throw new RemoteException("Failed to look up new neighbour", e);
        }
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        // Group 6 — exactly-once per jobId: concurrent duplicate submissions
        // (e.g. coordinator retries) collapse to a single execution. add() is
        // atomic on the concurrent set. Distinct jobIds proceed in parallel.
        // Note: the JAC is NOT incremented here. Per the spec and the Design
        // Decisions note, JAC counts assignments issued by a coordinator, not
        // work received. The coordinator increments it in submitJob().
        // Claim this jobId; duplicates exit here.
        if (!seenJobIds.add(jobId)) {
            return;
        }

        // Distributed chunks carry the original coordinator's address, so results
        // return correctly across neighbour hops and later leader changes.
        final Object calculationPayload = payload instanceof TaskAssignment assignment
                ? assignment.payload() : payload;
        final WorkerRemote resultReceiver = payload instanceof TaskAssignment assignment
                ? assignment.coordinator() : null;

        // Copy jobType for the background thread.
        final String typeSnapshot = jobType;
        // Count the task as active BEFORE handing it to the pool.
        // Recording it afterwards races the task's own cleanup: the pool thread
        // can finish first, its removal finds nothing to remove, and the entry
        // added afterwards is stranded - leaving getActiveTaskCount()
        // over-reporting for good and inflating the JAC this node advertises in
        // elections. Claiming it first means the task's removal always finds
        // something to remove; the rejection handler below removes it too.
        activeTasks.add(jobId);
        try {
            // Run the computation on a pool thread so this RMI call returns instantly.
            executor.submit(() -> {
                // Holder for the answer.
                Object computedResult;
                try {
                    // Delegate to the shared calculator (shares logic with the client preview).
                    computedResult = WorkerTaskCalculator.calculate(typeSnapshot, calculationPayload);
                } catch (Exception e) {
                    // Turn crashes into an error message.
                    computedResult = "Error executing task: " + e.getMessage();
                }

                try {
                    // Single volatile read: route on a consistent snapshot so a
                    // coordinator change mid-task cannot split the branch.
                    // One read of the leader.
                    int targetCoordinator = currentCoordinatorId;
                    // We are the leader (or none known): keep the result locally.
                    if (resultReceiver != null) {
                        resultReceiver.reportTaskResult(jobId, id, computedResult);
                    } else if (targetCoordinator == id || targetCoordinator == -1) {
                        reportTaskResult(jobId, id, computedResult);
                    } else {
                        // Look up the leader's connection.
                        WorkerRemote coordinatorStub = neighbours.get(targetCoordinator);
                        if (coordinatorStub != null) {
                            // Send the answer to the leader.
                            coordinatorStub.reportTaskResult(jobId, id, computedResult);
                        } else {
                            // Leader unreachable: store locally instead of dropping it.
                            reportTaskResult(jobId, id, computedResult);
                        }
                    }
                } catch (RemoteException ignored) {
                } finally {
                    // No longer running.
                    activeTasks.remove(jobId);
                }
            });
        } catch (RejectedExecutionException e) {
            // Pool shut down / saturated: release the claim so a retry can be accepted later.
            // Forget the tracking entry.
            activeTasks.remove(jobId);
            // Release the jobId claim so a later retry can run.
            seenJobIds.remove(jobId);
            // Tell the caller it was rejected.
            throw new RemoteException("Worker overloaded, task rejected for job " + jobId, e);
        }
    }

    @Override
    public void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException {
        JobState state = coordinatingJobs.get(jobId);
        if (state == null) {
            taskResults.putIfAbsent(new JobResultKey(jobId, workerId), result);
            return;
        }
        // Accept each expected worker only once. Store its answer before marking
        // it finished, so the last callback sees every partial result.
        synchronized (state) {
            if (!state.expected().contains(workerId)) return;
            taskResults.put(new JobResultKey(jobId, workerId), result);
            state.expected().remove(workerId);
        }
        finishJobIfReady(jobId, state);
    }

    private void finishJobIfReady(int jobId, JobState state) {
        // A fast calculation may finish while dispatch is still running. Wait for
        // both dispatch and every expected answer before sending one final result.
        synchronized (state) {
            if (!state.dispatchComplete().get() || !state.expected().isEmpty()
                    || !coordinatingJobs.remove(jobId, state)) return;
        }
        Object result = aggregateJob(state.jobType(), jobId);
        deliverToClient(jobId, result, state);
        maybeRotateCoordinator();
    }

    /**
     * Called by a Client via RMI. This node must currently be the coordinator:
     * it splits the payload across itself and its neighbours, tracks the pieces
     * in progress, and reports the aggregated result back to the client.
     */
    @Override
    public void submitJob(int jobId, String jobType, Object payload, String clientHost, int clientPort, String clientName) throws RemoteException {
        if (currentCoordinatorId != id) {
            throw new RemoteException("Worker " + id + " is not the current coordinator (coordinator=" + currentCoordinatorId + ")");
        }
        // Use bootstrap to find all registered workers without adding election
        // neighbours. Tests without bootstrap retain their directly connected peers.
        Map<Integer, WorkerRemote> available = discoverJobWorkers();
        List<Integer> participants = new ArrayList<>();
        participants.add(id);
        available.keySet().stream().filter(workerId -> workerId != id).sorted().forEach(participants::add);
        List<Object> chunks = splitPayload(jobType, payload, participants.size());
        if (!submittedJobIds.add(jobId)) return;

        // This job counts against the current term. Reserving here, after the
        // duplicate guard, means an accepted job is counted exactly once.
        // The reservation never writes a value above JOBS_PER_TERM, because
        // maybeRotateCoordinator() resets the counter concurrently: with an
        // increment-then-check scheme a request can observe 6, the reset can
        // land in between, and the late undo would drive the counter to -1 -
        // after which no term would ever reach its limit again.
        final boolean termExhausted = reserveTermSlot(jobId);

        // Register the entire job before any thread can report a result.
        Set<Integer> expected = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < participants.size(); i++) {
            if (chunks.get(i) != null) expected.add(participants.get(i));
        }
        JobState state = new JobState(jobType, expected, clientHost, clientPort, clientName, new AtomicBoolean());
        coordinatingJobs.put(jobId, state);
        WorkerRemote callback = (WorkerRemote) UnicastRemoteObject.toStub(this);

        final String typeSnapshot = jobType;
        for (int i = 0; i < participants.size(); i++) {
            Object chunk = chunks.get(i);
            if (chunk == null) {
                continue; // empty slice for this participant
            }
            int targetId = participants.get(i);
            if (targetId == id) {
                // Our own share: compute locally on a pool thread.
                final Object selfChunk = chunk;
                executor.submit(() -> {
                    Object result;
                    try {
                        result = WorkerTaskCalculator.calculate(typeSnapshot, selfChunk);
                    } catch (Exception e) {
                        result = "Error executing task: " + e.getMessage();
                    }
                    try {
                        reportTaskResult(jobId, id, result);
                    } catch (RemoteException ignored) {
                    }
                });
            } else {
                // Assigning to another worker: this is what counts against the JAC.
                jobAllocationCounter.incrementAndGet();
                WorkerRemote stub = available.get(targetId);
                if (stub == null) {
                    // The neighbour vanished mid-dispatch: compute its slice locally instead.
                    final Object slice = chunk;
                    executor.submit(() -> {
                        try {
                            reportTaskResult(jobId, targetId, WorkerTaskCalculator.calculate(typeSnapshot, slice));
                        } catch (RemoteException ignored) {
                        }
                    });
                    continue;
                }
                try {
                    stub.assignTask(jobId, typeSnapshot, new TaskAssignment(chunk, callback));
                } catch (RemoteException e) {
                    // Dead participant: forget it and compute its slice locally so
                    // the job still completes with the correct aggregate.
                    neighbours.remove(targetId);
                    final Object slice = chunk;
                    executor.submit(() -> {
                        Object fallback;
                        try {
                            fallback = WorkerTaskCalculator.calculate(typeSnapshot, slice);
                        } catch (Exception ex) {
                            fallback = "Error executing task: " + ex.getMessage();
                        }
                        try {
                            reportTaskResult(jobId, targetId, fallback);
                        } catch (RemoteException ignored) {
                        }
                    });
                }
            }
        }
        state.dispatchComplete().set(true);
        finishJobIfReady(jobId, state);
        if (termExhausted) {
            maybeRotateCoordinator();
        }
    }

    /**
     * Claims one job slot in the current term, rejecting once it is full.
     *
     * <p>Returns whether the caller filled the term and so has to hand over
     * leadership. A full term refuses the job rather than queueing it, so "at
     * most JOBS_PER_TERM jobs" holds under concurrent submissions. A refused job
     * gives its id back, so a client retry against the new coordinator is not
     * mistaken for a duplicate.
     */
    private boolean reserveTermSlot(int jobId) throws RemoteException {
        while (true) {
            int used = termJobsCoordinated.get();
            if (used >= Constants.JOBS_PER_TERM) {
                submittedJobIds.remove(jobId);
                throw new RemoteException("Worker " + id + " has coordinated the maximum of "
                        + Constants.JOBS_PER_TERM + " jobs for this term; it is handing over leadership");
            }
            if (termJobsCoordinated.compareAndSet(used, used + 1)) {
                return used + 1 == Constants.JOBS_PER_TERM;
            }
        }
    }

    private Map<Integer, WorkerRemote> discoverJobWorkers() throws RemoteException {
        Map<Integer, WorkerRemote> workers = new HashMap<>();
        BootstrapRemote directory = bootstrap;
        if (directory == null) {
            workers.putAll(neighbours);
        } else {
            for (int workerId : directory.getActiveWorkerIds()) {
                if (workerId == id) continue;
                WorkerAddress address = directory.getWorkerAddress(workerId);
                if (address == null) continue;
                try {
                    WorkerRemote worker = (WorkerRemote) LocateRegistry.getRegistry(
                            address.getRmiHost(), address.getRmiPort()).lookup("Worker" + workerId);
                    if (worker.isActive()) workers.put(workerId, worker);
                } catch (RemoteException | NotBoundException ignored) {
                    // Skip stale registrations; split the job among reachable workers.
                }
            }
        }
        workers.put(id, this);
        return workers;
    }

    /** Slice a payload into `parts` roughly equal slices; empty slices are null. */
    private List<Object> splitPayload(String jobType, Object payload, int parts) {
        List<Object> chunks = new ArrayList<>(WorkerTaskDistributor.split(jobType, payload, parts));
        while (chunks.size() < parts) {
            chunks.add(null);
        }
        return chunks;
    }

    /** Combine the per-worker pieces once every participant has reported. */
    private Object aggregateJob(String jobType, int jobId) {
        Map<Integer, Object> results = getJobResults(jobId);
        List<Number> partialResults = new ArrayList<>(results.size());
        for (Object result : results.values()) {
            if (!(result instanceof Number number)) {
                return result;
            }
            partialResults.add(number);
        }
        try {
            return WorkerTaskDistributor.combine(jobType, partialResults);
        } catch (ArithmeticException | IllegalArgumentException e) {
            return "Error aggregating task: " + e.getMessage();
        }
    }

    /** Send the final answer back to the client that submitted the job. */
    private void deliverToClient(int jobId, Object result, JobState state) {
        try {
            Registry clientRegistry = LocateRegistry.getRegistry(state.clientHost(), state.clientPort());
            ClientRemote client = (ClientRemote) clientRegistry.lookup(state.clientName());
            client.receiveJobResult(jobId, result);
        } catch (Exception e) {
            System.out.println("Worker " + id + ": could not deliver result for job " + jobId
                    + " to client : " + e.getMessage());
        }
    }

    /**
     * Term expiry: after Constants.JOBS_PER_TERM client jobs the coordinator
     * yields by starting a new election; the lowest-JAC worker wins and
     * leadership rotates. The counter is per job, not per assignment.
     */
    private void maybeRotateCoordinator() {
        // Reset the budget and start the election as one step. A plain
        // get()-then-set() let two threads both observe a full term and both
        // start an election, and since each election picks its own best
        // candidate from the JACs it happens to see, two of them can disagree
        // about the leader. The CAS makes exactly one thread win the reset, so
        // one term produces exactly one election; a value that is already reset
        // fails to match, which is the "someone else is rotating" case.
        while (true) {
            int used = termJobsCoordinated.get();
            if (used < Constants.JOBS_PER_TERM) {
                return;
            }
            if (termJobsCoordinated.compareAndSet(used, 0)) {
                startElection();
                return;
            }
        }
    }

    // ---- Heartbeat / coordinator-failure detection ----

    /** Wires the bootstrap reference and starts the periodic probe. */
    public void setBootstrapReference(BootstrapRemote bootstrap, String localHost) {
        this.bootstrap = bootstrap;
        heartbeatExecutor.scheduleAtFixedRate(this::heartbeatTick,
                Constants.HEARTBEAT_INTERVAL_MS, Constants.HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void ping() throws RemoteException {
        // Reachable = alive. No work to do; just being exported is enough.
    }

    private void heartbeatTick() {
        int coord = currentCoordinatorId;
        if (coord == -1 || coord == id || bootstrap == null) {
            return;
        }
        // Skip this beat if the last probe is still outstanding.
        if (!heartbeatInFlight.compareAndSet(false, true)) {
            return;
        }
        try {
            WorkerRemote stub = resolveCoordinatorStub(coord);
            if (stub == null) {
                return;
            }
            try {
                stub.ping();
            } catch (RemoteException e) {
                // Coordinator did not answer: treat it as dead and re-elect.
                coordinatorStub = null;
                startElection();
            }
        } catch (Exception e) {
            // Not bound, lookup failed, or bootstrap unreachable: assume the
            // coordinator is gone.
            coordinatorStub = null;
            startElection();
        } finally {
            heartbeatInFlight.set(false);
        }
    }

    private WorkerRemote resolveCoordinatorStub(int coord) {
        if (coordinatorStub != null && cachedCoordinatorId == coord) {
            return coordinatorStub;
        }
        try {
            WorkerAddress address = bootstrap.getWorkerAddress(coord);
            if (address == null) {
                return null;
            }
            Registry registry = LocateRegistry.getRegistry(address.getRmiHost(), address.getRmiPort());
            WorkerRemote stub = (WorkerRemote) registry.lookup("Worker" + coord);
            coordinatorStub = stub;
            cachedCoordinatorId = coord;
            return stub;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Group 6 lifecycle: stop accepting new tasks and finish in-flight ones.
     * Called from the WorkerNode shutdown hook; tests may also call it to avoid
     * leaking pool threads across test cases.
     */
    public void shutdown() {
        // Stop the pool (lets running jobs finish).
        executor.shutdown();
        // Stop the heartbeat scheduler (daemon, but clean shutdown is tidy).
        heartbeatExecutor.shutdown();
    }

    public boolean isShutdown() {
        // Report whether shutdown was called.
        return executor.isShutdown();
    }

    /** Latest result reported for a (jobId, workerId) pair, or null if none yet. */
    public Object getTaskResult(int jobId, int workerId) {
        // Look up one worker's answer.
        return taskResults.get(new JobResultKey(jobId, workerId));
    }

    /** Snapshot of all per-worker results collected locally for a job. Never null. */
    public Map<Integer, Object> getJobResults(int jobId) {
        // Fresh map for the answer.
        Map<Integer, Object> out = new HashMap<>();
        // Collect every worker's answer for this job.
        for (Map.Entry<JobResultKey, Object> e : taskResults.entrySet()) {
            if (e.getKey().jobId() == jobId) {
                out.put(e.getKey().workerId(), e.getValue());
            }
        }
        return out;
    }

    /** Number of in-flight (accepted but not yet finished) tasks. */
    public int getActiveTaskCount() {
        // Count running jobs.
        return activeTasks.size();
    }
}