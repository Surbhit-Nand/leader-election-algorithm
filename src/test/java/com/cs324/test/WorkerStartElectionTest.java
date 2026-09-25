package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step 2 of the worker test plan: startElection().
 *
 * An initiator has to seed its own election state with itself as the parent, so
 * that once every subtree has echoed back it is the node that decides the winner
 * and announces the coordinator.
 *
 * A lone worker has no neighbours, so there is nothing to wait for: the election
 * must resolve immediately to itself.
 *
 * These tests need no registry or peers - startElection on a worker with no
 * neighbours is entirely local.
 */
class WorkerStartElectionTest {

    /** How many back-to-back elections to start when checking that ids do not repeat. */
    private static final int RAPID_ELECTIONS = 100;

    private WorkerRemoteImpl worker;
    private WorkerRemoteImpl otherWorker;

    @AfterEach
    void tearDown() throws Exception {
        unexportQuietly(worker);
        unexportQuietly(otherWorker);
    }

    private void unexportQuietly(Remote remoteObject) {
        if (remoteObject == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(remoteObject, true);
        } catch (NoSuchObjectException ignored) {
            // already unexported, nothing to do
        }
    }

    /**
     * Gives a worker a starting Job Allocation Counter.
     *
     * Reflection is used deliberately: there is no public way to arrange JAC
     * values yet, and driving it through assignTask() would tie this test to
     * wherever that increment currently lives - which is exactly the thing that
     * is expected to move. Replace this helper once a proper seam exists.
     */
    private static void seedJobAllocationCounter(WorkerRemoteImpl worker, int value) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("jobAllocationCounter");
        field.setAccessible(true);
        ((AtomicInteger) field.get(worker)).set(value);
    }

    private static Set<Long> seenMessageIds(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("seenMessageIds");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<Long> ids = (Set<Long>) field.get(worker);
        return ids;
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Object> electionsOf(WorkerRemoteImpl worker) throws Exception {
        Field field = WorkerRemoteImpl.class.getDeclaredField("elections");
        field.setAccessible(true);
        return (Map<Long, Object>) field.get(worker);
    }

    /**
     * Reads one int off an ElectionState.
     *
     * ElectionState is a private nested class, so the field is looked up on the
     * value's own runtime class. This is inspection only - nothing here is used
     * to arrange state for the code under test.
     */
    private static int readElectionInt(Object electionState, String fieldName) throws Exception {
        Field field = electionState.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Integer) field.get(electionState);
    }

    @Test
    void aLoneWorkerWithNoNeighboursElectsItself() throws Exception {
        worker = new WorkerRemoteImpl(7);

        long messageId = worker.startElection();

        assertEquals(7, worker.getCurrentCoordinatorId(),
                "with no neighbours there is nobody to compete with, so the only candidate must win");
        assertTrue(messageId > 0, "startElection should return the id it used for this election");
    }

    @Test
    void everyElectionGetsItsOwnMessageId() throws Exception {
        worker = new WorkerRemoteImpl(7);

        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < RAPID_ELECTIONS; i++) {
            ids.add(worker.startElection());
        }

        assertEquals(RAPID_ELECTIONS, ids.size(),
                "every election must get a unique id - if two share one, every other worker drops the second as a "
                        + "duplicate and the re-election after a coordinator's term silently never happens");
    }

    @Test
    void differentWorkersGenerateDifferentElectionIds() throws Exception {
        worker = new WorkerRemoteImpl(4);
        otherWorker = new WorkerRemoteImpl(5);

        long firstId = worker.startElection();
        long secondId = otherWorker.startElection();

        assertNotEquals(firstId, secondId,
                "two different workers must not pick the same election id - they may be holding the same election at "
                        + "the same time, and one would be discarded as a duplicate of the other, so its subtree "
                        + "never echoes back and the two workers never agree on a coordinator");
    }

    @Test
    void startElectionInitializesElectionState() throws Exception {
        int initiatorId = 7;
        int startingJac = 3;
        worker = new WorkerRemoteImpl(initiatorId);
        seedJobAllocationCounter(worker, startingJac);

        long messageId = worker.startElection();

        assertTrue(seenMessageIds(worker).contains(messageId),
                "the initiator has processed its own election, so it must record the id as seen - otherwise the "
                        + "election echoed back around a cycle would look new to it and be processed a second time");

        Object election = electionsOf(worker).get(messageId);
        assertNotNull(election, "startElection must register an election under the id it returns; known ids: "
                + electionsOf(worker).keySet());

        assertEquals(initiatorId, readElectionInt(election, "parentId"),
                "the initiator is its own parent - a state pointing anywhere else makes the echo phase send its "
                        + "result to a neighbour that never asked for it, and the coordinator is never announced");
        assertEquals(initiatorId, readElectionInt(election, "bestCandidateId"),
                "the initiator is the first candidate it has seen, so it starts as its own best candidate");
        assertEquals(startingJac, readElectionInt(election, "bestCandidateJac"),
                "the election must start from the worker's actual JAC ("
                        + startingJac + ") - defaulting to 0 would give a busy worker a free pass to win every "
                        + "election it starts, and the whole JAC comparison in the echo phase would be decided "
                        + "against a number the worker never had");
    }
}
