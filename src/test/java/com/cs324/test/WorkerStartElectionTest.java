package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.rmi.NoSuchObjectException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @AfterEach
    void tearDown() throws Exception {
        if (worker != null) {
            try {
                UnicastRemoteObject.unexportObject(worker, true);
            } catch (NoSuchObjectException ignored) {
                // already unexported, nothing to do
            }
        }
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
}
