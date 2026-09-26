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
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
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
    // Holds the Job Allocation Counter. AtomicInteger = thread-safe counter. If removed/replaced with plain int, concurrent increments get lost.
    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    // Group 6: lock-free unique election IDs. Previously a plain long guarded by
    // synchronized startElection(); AtomicLong keeps IDs unique under concurrent
    // startElection() calls without serialising callers. Field name/type of
    // jobAllocationCounter is pinned by tests via reflection - do not rename it.
    // AtomicLong = thread-safe counter. If removed (plain long), two elections at once can get the same ID and be mistaken for duplicates.
    private final AtomicLong electionCounter = new AtomicLong(0);
    // The currently known coordinator. volatile = every thread always sees the newest value. If removed, threads can see a stale (old) coordinator.
    private volatile int currentCoordinatorId = -1; // -1 = no known coordinator yet
    // IDs of election messages already seen. Concurrent set = thread-safe. If removed, duplicates are processed again and messages loop forever.
    private final Set<Long> seenMessageIds = ConcurrentHashMap.newKeySet();
    // IDs of coordinator messages already seen. If removed, old coordinator announcements can overwrite newer ones.
    private final Set<Long> seenCoordinatorMessageIds = ConcurrentHashMap.newKeySet();
    // Ongoing elections by message ID. ConcurrentHashMap = safe for RMI threads. If removed (plain HashMap), concurrent access corrupts it.
    private final Map<Long, ElectionState> elections = new ConcurrentHashMap<>();
    // Known neighbours by worker ID. ConcurrentHashMap = safe to read while another thread adds a neighbour. If removed, adding + broadcasting at once can crash or skip peers.
    private final Map<Integer, WorkerRemote> neighbours = new ConcurrentHashMap<>();

    /** Composite key so concurrent results for the SAME jobId from DIFFERENT workers never clobber each other. */
    // Record = tiny container for (jobId, workerId). If removed (key by jobId only), results from different workers overwrite each other.
    private record JobResultKey(int jobId, int workerId) implements java.io.Serializable {}

    // Concurrency & Task tracking (Group 6)
    // Bounded pool with named daemon threads: concurrent jobs run in parallel,
    // a slow/long job never blocks unrelated jobs, and the pool can be shut down
    // cleanly via shutdown(). Daemon threads so a forgotten shutdown never hangs the JVM.
    // FixedThreadPool = limited workers running jobs in parallel. If removed (cached/unbounded pool), a burst of jobs spawns unlimited threads and crashes memory.
    private final ExecutorService executor = Executors.newFixedThreadPool(
            // Pool size = CPUs x 2 (min 4). Enough parallelism without overload. If removed (size 1), jobs run one-by-one and block each other.
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
            // ThreadFactory = names each thread. If removed, threads are anonymous and hard to debug.
            new ThreadFactory() {
                // Counter for thread names. AtomicInteger = safe numbering. If removed, two threads can get the same name.
                private final AtomicInteger seq = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    // Build one pool thread. If removed, no custom setup is applied.
                    Thread t = new Thread(r, "worker-task-" + seq.incrementAndGet());
                    // Daemon = JVM can exit even if threads are alive. If removed (non-daemon), the program hangs on exit.
                    t.setDaemon(true);
                    return t;
                }
            });
    // Per (jobId, workerId) results. ConcurrentHashMap gives lock-free thread-safe put/read.
    // If removed (plain map), concurrent result reports corrupt or lose data.
    private final Map<JobResultKey, Object> taskResults = new ConcurrentHashMap<>();
    // Exactly-once guard: jobIds already accepted (in-flight or done). add() is atomic,
    // so concurrent duplicate assignTask(jobId) calls collapse to a single execution.
    // Job IDs are assumed unique per job; a repeated jobId is treated as a retry and suppressed.
    // If removed, duplicate submissions each run the full computation (double work).
    private final Set<Integer> seenJobIds = ConcurrentHashMap.newKeySet();
    // In-flight tasks for lifecycle observability (not used for locking).
    // If removed, we cannot count or observe running jobs.
    private final Map<Integer, Future<?>> activeTasks = new ConcurrentHashMap<>();

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

    /** Local helper for the WorkerNode console menu (not part of the RMI contract). */
    public Set<Integer> getNeighbourIds() {
        return Set.copyOf(neighbours.keySet());
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
        // Build a unique message ID from worker ID + counter. AtomicLong = no two elections share an ID. If removed, concurrent elections collide and are dropped as duplicates.
        long messageId = ((long) id << 32) | (electionCounter.getAndIncrement());
        // Remember this ID so repeats are recognised as duplicates. If removed, our own message loops back and is processed again forever.
        seenMessageIds.add(messageId);

        // Read our JAC for the candidate comparison. Atomic get = always the newest value. If removed, we could campaign with a stale counter.
        int currentJac = jobAllocationCounter.get();
        // Create this wave's tracking object (starts with us as best candidate). If removed, there is nothing to merge echo replies into.
        ElectionState election = new ElectionState(this.id, this.id, currentJac);

        // Expect a reply from every current neighbour. If removed, the wave finishes early and the election never converges.
        for (Integer neighborId : neighbours.keySet()) {
            election.pendingChildren.add(neighborId);
        }

        // Publish the wave so echo replies can find it. If removed, incoming echoes find nothing and are ignored.
        elections.put(messageId, election);

        // Snapshot: blocking RMI calls run against a stable view; failures only
        // prune this wave's pendingChildren, never the shared neighbour map.
        // Copy of neighbours. If removed (loop live map), a neighbour joining mid-wave corrupts the broadcast.
        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        // Send the election message to every neighbour. If removed, nobody else hears about the election.
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            try {
                entry.getValue().receiveElectionMessage(this.id, this.id, currentJac, messageId);
            } catch (RemoteException e) {
                // Peer is dead: stop waiting for it. If removed, we wait forever for a dead peer.
                election.pendingChildren.remove(entry.getKey());
            }
        }

        // No neighbours (or all dead) = wave is done, send our echo up immediately. If removed, a lone worker never finishes its own election.
        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
        return messageId;
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId) throws RemoteException {
        // Duplicate check: add() returns false if already seen. If removed, the same wave is processed repeatedly and never terminates.
        if (!seenMessageIds.add(messageId)) {
            // Already seen: look up the wave. If removed (skip lookup), duplicates are silently dropped and neighbours wait forever.
            ElectionState existing = elections.get(messageId);
            if (existing != null) {
                try {
                    // Find who sent it. If removed, no reply is sent and the sender waits forever.
                    WorkerRemote sender = neighbours.get(originId);
                    if (sender != null) {
                        // Answer the duplicate with our current best so the sender can finish. If removed, triangle topologies deadlock.
                        sender.receiveEchoMessage(messageId, this.id, existing.bestCandidateId, existing.bestCandidateJac);
                    }
                } catch (RemoteException ignored) {}
            }
            return;
        }

        // Read our JAC for comparison. If removed, election uses a wrong/stale value and the wrong worker wins.
        int currentJac = jobAllocationCounter.get();
        // Start assuming the incoming candidate is best. If removed, there is no baseline to compare against.
        int bestId = candidateId;
        int bestJac = candidateJac;

        // Lower JAC wins (less-loaded worker leads). If removed, the busiest worker could win.
        if (currentJac < bestJac) {
            bestId = this.id;
            bestJac = currentJac;
        } else if (currentJac == bestJac) {
            // Tie: higher worker ID wins. If removed, ties never resolve to one agreed winner.
            if (this.id > bestId) {
                bestId = this.id;
                bestJac = currentJac;
            }
        }

        // Track this wave with us remembering who sent it (parent). If removed, the echo has nowhere to go back to.
        ElectionState election = new ElectionState(originId, bestId, bestJac);

        // Expect replies from all neighbours except the parent. If removed, we finish too early with incomplete data.
        for (Integer neighborId : neighbours.keySet()) {
            if (neighborId != originId) {
                election.pendingChildren.add(neighborId);
            }
        }

        // Publish the wave. If removed, echoes for it are ignored.
        elections.put(messageId, election);

        // Snapshot so concurrent neighbour changes mid-wave cannot corrupt fan-out.
        // Copy of neighbours. If removed, a concurrent join/leave corrupts this wave's broadcast.
        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        // Forward to every neighbour except the parent. If removed, workers beyond one hop never hear the election.
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            if (entry.getKey() != originId) {
                try {
                    entry.getValue().receiveElectionMessage(this.id, bestId, bestJac, messageId);
                } catch (RemoteException e) {
                    // Dead peer: stop waiting for it. If removed, one dead peer stalls the whole election.
                    election.pendingChildren.remove(entry.getKey());
                }
            }
        }

        // No children = leaf node, echo back immediately. If removed, leaf workers never reply and the wave stalls.
        if (election.pendingChildren.isEmpty()) {
            propagateEcho(messageId, election);
        }
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        // Drop repeats of the same announcement. If removed, the message bounces around the network forever.
        if (!seenCoordinatorMessageIds.add(messageId)) {
            return;
        }
        // Record the winner. Volatile write = all threads see it at once. If removed, this worker never learns the leader.
        this.currentCoordinatorId = coordinatorId;

        // Snapshot: relay runs on the calling RMI thread against a stable view.
        // Copy of neighbours. If removed, a concurrent neighbour change corrupts the relay.
        List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
        // Relay the winner to every neighbour so it reaches all hops. If removed, only direct neighbours learn the result.
        for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
            try {
                entry.getValue().receiveCoordinatorMessage(coordinatorId, messageId);
            } catch (RemoteException ignored) {}
        }
    }

    @Override
    public void receiveEchoMessage(long messageId, int fromWorkerId, int bestCandidateId, int bestCandidateJac) throws RemoteException {
        // Find the matching wave. If removed (skip lookup), the echo is applied to nothing and its data is lost.
        ElectionState election = elections.get(messageId);
        // Unknown wave (late/old echo): ignore. If removed (process anyway), it would crash on a null wave.
        if (election == null) {
            return;
        }

        // Lock this wave so two echoes arriving together cannot corrupt the merge. If removed, simultaneous echoes overwrite each other.
        synchronized (election) {
            // Keep the lower JAC. If removed, a worse candidate can overwrite a better one.
            if (bestCandidateJac < election.bestCandidateJac) {
                election.bestCandidateId = bestCandidateId;
                election.bestCandidateJac = bestCandidateJac;
            } else if (election.bestCandidateJac == bestCandidateJac) {
                // Tie: keep the higher ID. If removed, tied workers disagree on the winner.
                if (bestCandidateId > election.bestCandidateId) {
                    election.bestCandidateId = bestCandidateId;
                    election.bestCandidateJac = bestCandidateJac;
                }
            }

            // One child answered, stop waiting for it. If removed, this wave never completes.
            election.pendingChildren.remove(fromWorkerId);

            // All children answered and we haven't echoed yet: echo upward. If removed, the result never travels back to the starter.
            if (election.pendingChildren.isEmpty() && !election.echoed) {
                propagateEcho(messageId, election);
            }
        }
    }

    private void propagateEcho(long messageId, ElectionState election) {
        // Lock + claim the echo so it is sent exactly once. If removed, duplicate echoes announce two different winners.
        synchronized (election) {
            if (election.echoed) {
                return;
            }
            election.echoed = true;
        }

        // We started this wave: we now know the winner. If removed, the starter never learns the result.
        if (election.parentId == this.id) {
            // Save the winner locally. If removed, this worker disagrees with everyone else.
            this.currentCoordinatorId = election.bestCandidateId;
            // Coordinator announcement ID derived from the election ID. If removed, the announcement has no ID and dedup breaks.
            long coordMessageId = messageId + 1000000L;
            // Snapshot of neighbours for the broadcast. If removed, concurrent neighbour changes corrupt the announcement.
            List<Map.Entry<Integer, WorkerRemote>> snapshot = new ArrayList<>(neighbours.entrySet());
            // Announce the winner to all neighbours. If removed, the election result never leaves this node.
            for (Map.Entry<Integer, WorkerRemote> entry : snapshot) {
                try {
                    entry.getValue().receiveCoordinatorMessage(election.bestCandidateId, coordMessageId);
                } catch (RemoteException ignored) {}
            }
        } else {
            // Forward our best upward to our parent. If removed, the parent waits forever and the election stalls.
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
            // Contact the new neighbour's registry. If removed, we cannot find the neighbour at all.
            Registry registry = LocateRegistry.getRegistry(rmiHost, rmiPort);
            // Look up its worker object. If removed, we store nothing and the link is never made.
            WorkerRemote stub = (WorkerRemote) registry.lookup("Worker" + newWorker);
            // Save it (overwrites any old entry for that ID). If removed, the neighbour is forgotten immediately.
            neighbours.put(newWorker, stub);
        } catch (NotBoundException e) {
            // Name not found: report failure. If removed, callers think a missing neighbour was added.
            throw new RemoteException("Failed to look up new neighbour", e);
        }
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        // Group 6 — exactly-once per jobId: concurrent duplicate submissions
        // (e.g. coordinator retries) collapse to a single execution. add() is
        // atomic on the concurrent set. Distinct jobIds proceed in parallel.
        // The JAC increments exactly once per accepted (first-seen) job.
        // Claim this jobId; duplicates exit here. If removed, every retry re-runs the whole computation (double work).
        if (!seenJobIds.add(jobId)) {
            return;
        }
        // Count this accepted job. Atomic = no lost increments. If removed, elections use a wrong JAC and pick the wrong leader.
        jobAllocationCounter.incrementAndGet();

        // Copy jobType for the background thread. If removed, a changed variable could send the wrong computation.
        final String typeSnapshot = jobType;
        try {
            // Run the computation on a pool thread so this RMI call returns instantly. If removed (compute directly), one slow job blocks all other jobs.
            Future<?> future = executor.submit(() -> {
                // Holder for the answer. If removed, there is nowhere to store the result.
                Object computedResult;
                try {
                    // Pick the computation by type. If removed, every job runs the same (wrong) calculation.
                    if ("MAX".equalsIgnoreCase(typeSnapshot)) {
                        computedResult = computeMax(payload);
                    } else if ("PRIMESUM".equalsIgnoreCase(typeSnapshot)) {
                        computedResult = computePrimeSum(payload);
                    } else if ("PRIMECOUNT".equalsIgnoreCase(typeSnapshot)) {
                        computedResult = computePrimeCount(payload);
                    } else {
                        computedResult = "Unsupported job type: " + typeSnapshot;
                    }
                } catch (Exception e) {
                    // Turn crashes into an error message. If removed, one bad payload kills the thread with no result.
                    computedResult = "Error executing task: " + e.getMessage();
                }

                try {
                    // Single volatile read: route on a consistent snapshot so a
                    // coordinator change mid-task cannot split the branch.
                    // One read of the leader. If removed (two reads), the leader can change between reads and the result goes to the wrong node.
                    int targetCoordinator = currentCoordinatorId;
                    // We are the leader (or none known): keep the result locally. If removed, results are always sent over the network, even to ourselves.
                    if (targetCoordinator == id || targetCoordinator == -1) {
                        reportTaskResult(jobId, id, computedResult);
                    } else {
                        // Look up the leader's connection. If removed, results can never be forwarded.
                        WorkerRemote coordinatorStub = neighbours.get(targetCoordinator);
                        if (coordinatorStub != null) {
                            // Send the answer to the leader. If removed, the leader never receives this piece of the job.
                            coordinatorStub.reportTaskResult(jobId, id, computedResult);
                        } else {
                            // Leader unreachable: store locally instead of dropping it. If removed, the result is silently lost.
                            reportTaskResult(jobId, id, computedResult);
                        }
                    }
                } catch (RemoteException ignored) {
                } finally {
                    // No longer running. If removed, the active-task count grows forever and looks like everything is stuck.
                    activeTasks.remove(jobId);
                }
            });
            // Track the running task. If removed, we cannot observe or count in-flight jobs.
            activeTasks.put(jobId, future);
        } catch (RejectedExecutionException e) {
            // Pool shut down / saturated: release the claim so a retry can be accepted later.
            // Forget the tracking entry. If removed, a dead entry blocks that jobId forever.
            activeTasks.remove(jobId);
            // Release the jobId claim so a later retry can run. If removed, that job can never be retried.
            seenJobIds.remove(jobId);
            // Tell the caller it was rejected. If removed, the caller waits forever for a result that will never come.
            throw new RemoteException("Worker overloaded, task rejected for job " + jobId, e);
        }
    }

    @Override
    public void reportTaskResult(int jobId, int workerId, Object result) throws RemoteException {
        // Keyed by (jobId, workerId): concurrent reports for the same job from
        // different workers no longer overwrite each other (no lost updates).
        // Save one worker's answer. If removed, results are never stored and the job looks unfinished.
        taskResults.put(new JobResultKey(jobId, workerId), result);
    }

    /**
     * Group 6 lifecycle: stop accepting new tasks and finish in-flight ones.
     * Called from the WorkerNode shutdown hook; tests may also call it to avoid
     * leaking pool threads across test cases.
     */
    public void shutdown() {
        // Stop the pool (lets running jobs finish). If removed, threads leak and the JVM hangs on exit.
        executor.shutdown();
    }

    public boolean isShutdown() {
        // Report whether shutdown was called. If removed, callers cannot check pool state.
        return executor.isShutdown();
    }

    /** Latest result reported for a (jobId, workerId) pair, or null if none yet. */
    public Object getTaskResult(int jobId, int workerId) {
        // Look up one worker's answer. If removed, there is no way to read a single result.
        return taskResults.get(new JobResultKey(jobId, workerId));
    }

    /** Snapshot of all per-worker results collected locally for a job. Never null. */
    public Map<Integer, Object> getJobResults(int jobId) {
        // Fresh map for the answer. If removed, we would leak our internal map to callers.
        Map<Integer, Object> out = new HashMap<>();
        // Collect every worker's answer for this job. If removed, only one worker's answer is ever visible.
        for (Map.Entry<JobResultKey, Object> e : taskResults.entrySet()) {
            if (e.getKey().jobId() == jobId) {
                out.put(e.getKey().workerId(), e.getValue());
            }
        }
        return out;
    }

    /** Number of in-flight (accepted but not yet finished) tasks. */
    public int getActiveTaskCount() {
        // Count running jobs. If removed, we cannot tell whether the worker is busy or idle.
        return activeTasks.size();
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