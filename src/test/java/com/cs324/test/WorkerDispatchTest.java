package com.cs324.test;


import com.cs324.common.Constants;
import com.cs324.worker.WorkerRemoteImpl;
import com.cs324.worker.WorkerTaskCalculator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.lang.reflect.Method;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Client -> coordinator job submission over real RMI.
 *
 * A 3-worker ring (1-3) elects worker 1 as coordinator; the client submits
 * MAX / PRIMESUM / PRIMECOUNT jobs. The coordinator splits each payload across
 * itself and its neighbours, aggregates the sub-results and delivers the final
 * answer to a recording ClientRemote. JAC is asserted coordinator-side: it
 * counts only assignments issued to OTHER workers.
 */
class WorkerDispatchTest {

    private static final int TEST_RMI_PORT = 19808;
    private static final String HOST = "localhost";
    private static final String CLIENT_NAME = "Client";
    private static final long AWAIT_TIMEOUT_MS = 10_000;

    private static Registry registry;

    private WorkerRemoteImpl w1;
    private WorkerRemoteImpl w2;
    private WorkerRemoteImpl w3;
    private RecordingClientRemote client;

    @BeforeAll
    static void startRegistry() throws RemoteException {
        registry = LocateRegistry.createRegistry(TEST_RMI_PORT);
    }

    @AfterAll
    static void stopRegistry() throws Exception {
        UnicastRemoteObject.unexportObject(registry, true);
    }

    @BeforeEach
    void setUp() throws Exception {
        w1 = new WorkerRemoteImpl(1);
        w2 = new WorkerRemoteImpl(2);
        w3 = new WorkerRemoteImpl(3);
        client = new RecordingClientRemote();

        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        registry.rebind("Worker3", w3);
        registry.rebind(CLIENT_NAME, client);

        // Full ring so the coordinator knows every participant.
        w1.connectAsNeighbour(2, HOST, TEST_RMI_PORT);
        w1.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
        w2.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        w2.connectAsNeighbour(3, HOST, TEST_RMI_PORT);
        w3.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        w3.connectAsNeighbour(2, HOST, TEST_RMI_PORT);

        // Elect worker 1 as the coordinator on every node.
        w1.receiveCoordinatorMessage(1, 100L);
        w2.receiveCoordinatorMessage(1, 101L);
        w3.receiveCoordinatorMessage(1, 102L);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            try {
                registry.unbind(name);
            } catch (Exception ignored) {
            }
        }
        for (WorkerRemoteImpl worker : new WorkerRemoteImpl[]{w1, w2, w3}) {
            if (worker != null) {
                worker.shutdown();
            }
        }
        unexportQuietly(w1);
        unexportQuietly(w2);
        unexportQuietly(w3);
        unexportQuietly(client);
    }

    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
        }
    }

    private void awaitResult(int jobId) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (client.received(jobId)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("job " + jobId + " never returned within " + AWAIT_TIMEOUT_MS + "ms");
    }

    @Test
    void maxIsCalculatedAcrossSplitPayloads() throws Exception {
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);
        assertEquals(22, client.resultFor(1));
    }

    @Test
    void primeSumIsAggregatedAcrossRanges() throws Exception {
        w1.submitJob(2, "PRIMESUM", new WorkerTaskCalculator.PrimeRange(1, 20),
                HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(2);
        assertEquals(77L, client.resultFor(2));
    }

    @Test
    void primeCountIsAggregatedAcrossLists() throws Exception {
        w1.submitJob(3, "PRIMECOUNT", new int[]{2, 4, 6, 8, 10, 11, 13},
                HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(3);
        assertEquals(3, client.resultFor(3));
    }

    @Test
    void coordinatorJacCountsOnlyAssignmentsToOtherWorkers() throws Exception {
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);

        // Two of the three slices went to workers 2 and 3.
        assertEquals(2, w1.getJobAllocationCounter(),
                "coordinator JAC counts each assignment to another worker");
        assertEquals(0, w2.getJobAllocationCounter(),
                "a receiving worker never counts assigned work");
        assertEquals(0, w3.getJobAllocationCounter(),
                "a receiving worker never counts assigned work");
    }

    @Test
    void nonCoordinatorRejectsJobs() throws Exception {
        RemoteException thrown = assertThrows(RemoteException.class, () ->
                w2.submitJob(9, "MAX", new int[]{1, 2, 3}, HOST, TEST_RMI_PORT, CLIENT_NAME));
        assertEquals(0, client.callCount(),
                "no result may be delivered for a job the coordinator rejected");
    }

    @Test
    void deadParticipantSliceIsComputedLocally() throws Exception {
        // Sanity: a full job first, so the participant set is established.
        w1.submitJob(1, "MAX", new int[]{4, 11, 2, 19, 8, 1, 22, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(1);
        assertEquals(22, client.resultFor(1));

        // Worker 2 "dies" (unexported) after the first job.
        UnicastRemoteObject.unexportObject(w2, true);

        w1.submitJob(5, "MAX", new int[]{30, 7, 1, 300, 12, 2}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        awaitResult(5);

        assertEquals(300, client.resultFor(5),
                "a dead participant's slice must be recomputed by the coordinator so the "
                        + "aggregate stays correct");
    }

    @Test
    void coordinatorKeepsLeadingUntilTheFifthJobOfItsTerm() throws Exception {
        // A term is JOBS_PER_TERM (5) client jobs, not 5 task assignments.
        // Each job here assigns two slices (to w2 and w3), so the old
        // per-assignment reading rotated the leader after only 2 jobs.
        for (int j = 1; j <= 4; j++) {
            w1.submitJob(200 + j, "MAX", new int[]{j, j + 7, 42}, HOST, TEST_RMI_PORT, CLIENT_NAME);
            awaitResult(200 + j);
        }

        assertEquals(1, w1.getCurrentCoordinatorId(),
                "the coordinator must still be leading during its first four jobs of the term");
        assertEquals(4, w1.getJobsCoordinatedInTerm(),
                "each accepted job counts once against the term, regardless of how many workers it is split across");
        assertEquals(8, w1.getJobAllocationCounter(),
                "the JAC still counts every assignment issued to another worker");
    }

    @Test
    void coordinatorRotatesAfterItsTermTrigger() throws Exception {
        // A term is JOBS_PER_TERM (5) client jobs. All workers start at JAC 0, so
        // after the fifth job the coordinator holds JAC 10 while w2/w3 are still
        // on 0, and the lowest-JAC tie-break hands leadership to worker 3.
        for (int j = 1; j <= 5; j++) {
            w1.submitJob(100 + j, "MAX", new int[]{j, j + 7, 42}, HOST, TEST_RMI_PORT, CLIENT_NAME);
            awaitResult(100 + j);
        }

        final long deadline = System.currentTimeMillis() + 8_000;
        while (System.currentTimeMillis() < deadline) {
            if (w1.getCurrentCoordinatorId() != 1) {
                break;
            }
            Thread.sleep(50);
        }

        assertEquals(3, w1.getCurrentCoordinatorId(),
                "after JOBS_PER_TERM client jobs the coordinator must rotate to the "
                        + "lowest-JAC worker (w3), got coordinator=" + w1.getCurrentCoordinatorId());
        assertEquals(10, w1.getJobAllocationCounter(),
                "the departed coordinator keeps the JAC it earned during its term");
    }

    @Test
    void aTermRefusesASixthJobWhileItsLastSlotIsStillBeingDispatched() throws Exception {
        BlockingAssignWorkerRemote slow2 = installBlockingParticipant(2);
        installBlockingParticipant(3);

        // Four accepted jobs leave the term one slot short. The participants never
        // report a result, so none of these jobs can finish and the term cannot
        // rotate out from under the test.
        for (int job = 401; job <= 404; job++) {
            w1.submitJob(job, "MAX", new int[]{job, job + 1, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        }
        assertEquals(4, w1.getJobsCoordinatedInTerm(),
                "each accepted job takes exactly one slot of the term's budget");

        // The fifth job claims the last slot and then parks inside dispatch, so the
        // budget is spent but not yet reset. This is the window a sixth concurrent
        // submission used to slip through, taking the term to six jobs.
        slow2.blockJob(405);
        Thread fifth = new Thread(() -> {
            try {
                w1.submitJob(405, "MAX", new int[]{405, 406, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
            } catch (RemoteException ignored) {
            }
        }, "fifth-submit");
        fifth.start();
        awaitTrue(() -> w1.getJobsCoordinatedInTerm() == 5,
                "the fifth job should have claimed the term's last slot");

        RemoteException refusal = assertThrows(RemoteException.class, () ->
                        w1.submitJob(406, "MAX", new int[]{406, 407, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME),
                "a sixth job must not be accepted into a term that has already served its budget");
        assertTrue(String.valueOf(refusal.getMessage()).contains("term"),
                "the refusal should say the term is spent, got: " + refusal.getMessage());
        assertEquals(5, w1.getJobsCoordinatedInTerm(),
                "a refused job must hand its slot back instead of consuming one");

        slow2.release();
        fifth.join(AWAIT_TIMEOUT_MS);
    }

    @Test
    void concurrentSubmissionsNeverOverfillATerm() throws Exception {
        BlockingAssignWorkerRemote quiet2 = installBlockingParticipant(2);
        installBlockingParticipant(3);

        long electionsBefore = electionCounterValue(w1);
        final int submissions = 6;
        CountDownLatch armed = new CountDownLatch(submissions);
        CountDownLatch fire = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        List<Throwable> refused = Collections.synchronizedList(new ArrayList<>());
        List<Thread> callers = new ArrayList<>();

        for (int j = 0; j < submissions; j++) {
            final int jobId = 500 + j;
            Thread caller = new Thread(() -> {
                armed.countDown();
                try {
                    fire.await();
                    w1.submitJob(jobId, "MAX", new int[]{jobId, jobId + 1, 7},
                            HOST, TEST_RMI_PORT, CLIENT_NAME);
                    accepted.incrementAndGet();
                } catch (Throwable failure) {
                    refused.add(failure);
                }
            }, "submit-" + jobId);
            callers.add(caller);
            caller.start();
        }

        assertTrue(armed.await(10, TimeUnit.SECONDS), "every submission should be waiting at the gate");
        fire.countDown();
        for (Thread caller : callers) {
            caller.join(AWAIT_TIMEOUT_MS);
        }

        // No job ever completes here, so the term can only be ended by the
        // reservation itself. That makes the number of elections a direct measure
        // of how many terms were spent, and a term is never allowed to hold more
        // than JOBS_PER_TERM jobs.
        long elections = electionCounterValue(w1) - electionsBefore;
        assertTrue(elections >= 1,
                "a spent term must trigger an election; the counter moved by " + elections);
        assertEquals(elections, countCalls(quiet2.calls(), "election|"),
                "every election started should have reached the participants exactly once; recorded on "
                        + "worker 2: " + quiet2.calls());
        assertTrue(accepted.get() <= 5L * elections,
                "each term accepts at most 5 jobs, so " + accepted.get() + " accepted jobs need at least "
                        + ((accepted.get() + 4) / 5) + " elections, but only " + elections
                        + " were started");
        assertEquals(submissions, accepted.get() + refused.size(),
                "every submission must be either accepted or refused, never silently dropped");
    }

    @Test
    void aTermRefusesAJobWhileLeadershipIsBeingHandedOver() throws Exception {
        BlockingAssignWorkerRemote stalled2 = installBlockingParticipant(2);
        installBlockingParticipant(3);

        // Five accepted jobs spend the term and the fifth starts the handover.
        // These participants answer an election by recording it and never echo
        // it, so the wave never concludes: the budget has already been reset to
        // zero while this node is still the named coordinator. That is the
        // window a sixth job used to walk straight into, because the only thing
        // guarding submitJob was the coordinator id, and that still read "me".
        for (int job = 601; job <= 605; job++) {
            w1.submitJob(job, "MAX", new int[]{job, job + 1, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        }

        assertEquals(0, w1.getJobsCoordinatedInTerm(),
                "the fifth job spends the term and resets the budget");
        assertEquals(1, w1.getCurrentCoordinatorId(),
                "the wave has stalled, so this node is still named coordinator");
        assertTrue(countCalls(stalled2.calls(), "election|") >= 1,
                "the fifth job should have started the handover election; recorded on worker 2: "
                        + stalled2.calls());

        RemoteException refusal = assertThrows(RemoteException.class, () ->
                        w1.submitJob(606, "MAX", new int[]{606, 607, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME),
                "a job arriving mid-handover must not be accepted into the term that has just ended");
        assertTrue(String.valueOf(refusal.getMessage()).contains("handing over"),
                "the refusal should say a handover is in progress, got: " + refusal.getMessage());
        assertEquals(0, w1.getJobsCoordinatedInTerm(),
                "a refused job must not consume any of the incoming term's budget");
    }

    @Test
    void aJoiningWorkerDoesNotResetTheServingCoordinatorsTerm() throws Exception {
        // Four accepted jobs: this term is part-way through its budget, which is
        // exactly the state a second budget must not be handed out in.
        for (int job = 701; job <= 704; job++) {
            w1.submitJob(job, "MAX", new int[]{job, job + 1, 7}, HOST, TEST_RMI_PORT, CLIENT_NAME);
        }
        assertEquals(4, w1.getJobsCoordinatedInTerm(), "four jobs leave this term part-spent");

        // A worker joining an existing cluster adopts the leader its peer already
        // knows - WorkerNode calls adoptCoordinator, which is local-only. A joiner
        // is wired to a single peer, and that peer already knows the coordinator,
        // so re-broadcasting it reaches the serving coordinator under a fresh
        // message id, which it cannot tell from a new term: it restarted its
        // budget and served nine jobs in one term instead of five.
        WorkerRemoteImpl joiner = new WorkerRemoteImpl(9);
        registry.rebind("Worker9", joiner);
        joiner.connectAsNeighbour(1, HOST, TEST_RMI_PORT);
        try {
            joiner.adoptCoordinator(1, 900L);

            assertEquals(1, joiner.getCurrentCoordinatorId(),
                    "the joiner should adopt the coordinator it was told about");
            assertEquals(4, w1.getJobsCoordinatedInTerm(),
                    "a joiner adopting the current coordinator must not restart the term it is joining");
        } finally {
            joiner.shutdown();
            unexportQuietly(joiner);
        }
    }

    @Test
    void adoptingACoordinatorIsNotRelayedToNeighbours() throws Exception {
        RecordingWorkerRemote peer = new RecordingWorkerRemote(8);
        registry.rebind("Worker8", peer);

        WorkerRemoteImpl joiner = new WorkerRemoteImpl(9);
        registry.rebind("Worker9", joiner);
        joiner.connectAsNeighbour(8, HOST, TEST_RMI_PORT);
        try {
            joiner.adoptCoordinator(8, 900L);

            assertEquals(8, joiner.getCurrentCoordinatorId(), "the joiner should know the coordinator");
            assertEquals(0, countCalls(peer.calls(), "coordinator|"),
                    "adoption is local: the peer it learned from already knows this coordinator, so "
                            + "relaying would only spread an announcement nobody needs; recorded on worker 8: "
                            + peer.calls());
        } finally {
            joiner.shutdown();
            unexportQuietly(joiner);
            unexportQuietly(peer);
        }
    }

    @Test
    void aReservationRacingARotationKeepsTheTermBudgetInRange() throws Exception {
        // reserveTermSlot and maybeRotateCoordinator are private, and the state
        // they race over is not reachable from outside, so both are driven here.
        //
        // The CAS keeps the budget inside [0, JOBS_PER_TERM] at every instant: a
        // reservation can only move the counter from a value it has already
        // checked to be under the cap, so the counter is never observable above
        // the cap, and a refused reservation never has to undo anything - so
        // there is no late decrement that could land after a rotation has already
        // reset the counter and drive it below zero. An increment-then-check
        // scheme breaks both halves of that: it publishes the over-cap value for
        // as long as the check takes, and its undo can land out of order.
        final int reservers = 4;
        final int rotators = 2;
        final int iterations = 4000;

        setTermCount(w1, 0);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger highest = new AtomicInteger(0);
        AtomicInteger lowest = new AtomicInteger(Constants.JOBS_PER_TERM);

        Thread watcher = new Thread(() -> {
            while (!stop.get()) {
                int seen = termCountUnchecked(w1);
                highest.accumulateAndGet(seen, Math::max);
                lowest.accumulateAndGet(seen, Math::min);
            }
        }, "budget-watcher");
        watcher.setDaemon(true);
        watcher.start();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < reservers; i++) {
            Thread thread = new Thread(() -> {
                for (int n = 0; n < iterations; n++) {
                    try {
                        reserveSlot(w1, 9000 + n);
                    } catch (Exception refused) {
                        // A full term refuses; that is an expected outcome here.
                    }
                }
            }, "reserve-" + i);
            threads.add(thread);
            thread.start();
        }
        for (int i = 0; i < rotators; i++) {
            Thread thread = new Thread(() -> {
                for (int n = 0; n < iterations; n++) {
                    try {
                        rotateIfTermSpent(w1);
                    } catch (Exception ignored) {
                        // Reported through the budget, which is what is asserted.
                    }
                }
            }, "rotate-" + i);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join(AWAIT_TIMEOUT_MS);
        }
        stop.set(true);
        watcher.join(AWAIT_TIMEOUT_MS);

        int high = highest.get();
        int low = lowest.get();
        assertTrue(low >= 0,
                "the term budget was observed at " + low + ", below zero. A refused reservation's late "
                        + "decrement has to land after a rotation that already reset the counter, and from a "
                        + "negative budget a term that already served its share keeps admitting jobs");
        assertTrue(high <= Constants.JOBS_PER_TERM,
                "the term budget was observed at " + high + ", above the cap of " + Constants.JOBS_PER_TERM
                        + ". Publishing an over-cap value means the counter was moved before it was checked, "
                        + "so concurrent readers see a spent term as having room left");
    }

    /** Reads the term budget without the checked reflection of termCount(). */
    private static int termCountUnchecked(WorkerRemoteImpl worker) {
        try {
            return termCount(worker);
        } catch (Exception unreadable) {
            return 0;
        }
    }

    @Test
    void aSpentTermStartsExactlyOneElectionHoweverManyThreadsNoticeIt() throws Exception {
        // maybeRotateCoordinator is private, and the race it guards is only
        // reachable from inside the node, so it is driven directly: put the term
        // into the spent state and let several threads notice it at once.
        //
        // Resetting the budget with get()-then-set() let two threads both observe
        // a spent term and both start an election. That is not merely a wasted
        // wave: each election computes its own best candidate from the JACs it
        // sees, so two elections for one term can settle on different leaders.
        // Whichever result a node applies last wins, so the cluster can end up
        // disagreeing with itself.
        //
        // The budget is reset and the election started as a single CAS step, so
        // exactly one thread can win the reset and exactly one election runs, no
        // matter how many threads were about to rotate.
        final int rounds = 500;
        final int racers = 8;
        long electionsBefore = electionCounterValue(w1);

        for (int round = 0; round < rounds; round++) {
            markTermSpent(w1);
            CountDownLatch armed = new CountDownLatch(racers);
            CountDownLatch fire = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>();
            for (int racer = 0; racer < racers; racer++) {
                Thread thread = new Thread(() -> {
                    armed.countDown();
                    try {
                        fire.await();
                        rotateIfTermSpent(w1);
                    } catch (Exception ignored) {
                        // Reported through the election counter, which is the
                        // thing under test.
                    }
                }, "rotate-" + round + "-" + racer);
                threads.add(thread);
                thread.start();
            }
            assertTrue(armed.await(10, TimeUnit.SECONDS), "every racer should be waiting at the gate");
            fire.countDown();
            for (Thread thread : threads) {
                thread.join(AWAIT_TIMEOUT_MS);
            }

            long spent = electionCounterValue(w1) - electionsBefore;
            assertEquals(round + 1L, spent,
                    "round " + round + ": " + racers + " threads found the term spent, but a term may only be "
                            + "handed over once - " + (spent - (round + 1L)) + " extra election(s) were started, so "
                            + "two threads both won the reset and are running rival elections that can disagree on "
                            + "the leader");
        }
    }

    /** Puts the node's term budget into the spent state a rotation reacts to. */
    private static void markTermSpent(WorkerRemoteImpl worker) throws Exception {
        setTermCount(worker, Constants.JOBS_PER_TERM);
    }

    /** Writes the term budget directly, as a served job's reservation would. */
    private static void setTermCount(WorkerRemoteImpl worker, int jobs) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("termJobsCoordinated");
        field.setAccessible(true);
        ((AtomicInteger) field.get(worker)).set(jobs);
    }

    private static int termCount(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("termJobsCoordinated");
        field.setAccessible(true);
        return ((AtomicInteger) field.get(worker)).get();
    }

    /** Invokes the private reservation, which submits no work of its own. */
    private static boolean reserveSlot(WorkerRemoteImpl worker, int jobId) throws Exception {
        Method method = WorkerRemoteImpl.class.getDeclaredMethod("reserveTermSlot", int.class);
        method.setAccessible(true);
        return (boolean) method.invoke(worker, jobId);
    }

    /** Invokes the private rotation hook directly, as a finishing job would. */
    private static void rotateIfTermSpent(WorkerRemoteImpl worker) throws Exception {
        Method method = WorkerRemoteImpl.class.getDeclaredMethod("maybeRotateCoordinator");
        method.setAccessible(true);
        method.invoke(worker);
    }

    /**
     * Rebinds a participant as a worker that records without reporting, so jobs
     * dispatched to it never complete. connectAsNeighbour overwrites by id, which
     * is what lets the coordinator pick the double up as its participant.
     */
    private BlockingAssignWorkerRemote installBlockingParticipant(int workerId) throws Exception {
        BlockingAssignWorkerRemote participant = new BlockingAssignWorkerRemote(workerId);
        registry.rebind("Worker" + workerId, participant);
        w1.connectAsNeighbour(workerId, HOST, TEST_RMI_PORT);
        return participant;
    }

    /** Reads the private election counter, i.e. how many elections were started. */
    private static long electionCounterValue(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("electionCounter");
        field.setAccessible(true);
        return ((AtomicLong) field.get(worker)).get();
    }

    private static long countCalls(List<String> calls, String prefix) {
        return calls.stream().filter(call -> call.startsWith(prefix)).count();
    }

    private static void awaitTrue(BooleanSupplier condition, String failureMessage) throws Exception {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        fail(failureMessage);
    }
}