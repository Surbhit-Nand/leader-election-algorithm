package com.cs324.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.rmi.NoSuchObjectException;
import java.rmi.server.UnicastRemoteObject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cs324.worker.WorkerRemoteImpl;

/**
 * WorkerRemoteImpl extends UnicastRemoteObject, so constructing it exports
 * it via RMI on an anonymous port as a side effect. These tests call its
 * methods directly (no registry lookup needed - it's just a local object
 * reference within the same JVM), but still need to unexport it afterward
 * so repeated test runs don't leak RMI resources.
 */
class WorkerRemoteImplTest {

    private WorkerRemoteImpl worker;

    @AfterEach
    void tearDown() {
        if (worker != null) {
            try {
                UnicastRemoteObject.unexportObject(worker, true);
            } catch (NoSuchObjectException ignored) {
                // already unexported, nothing to do
            }
        }
    }

    @Test
    void newCoordinatorMessageUpdatesCurrentCoordinator() throws Exception {
        worker = new WorkerRemoteImpl(5);

        worker.receiveCoordinatorMessage(7, 100L);

        assertEquals(7, worker.getCurrentCoordinatorId(),
                "receiving a fresh COORDINATOR message should update who this worker believes is coordinator");
    }

    @Test
    void duplicateCoordinatorMessageIsIgnoredEvenWithADifferentCoordinatorId() throws Exception {
        worker = new WorkerRemoteImpl(5);
        worker.receiveCoordinatorMessage(7, 100L);

        // same messageId as before, but claiming a DIFFERENT coordinator -
        // this should be dropped as a duplicate, not applied
        worker.receiveCoordinatorMessage(99, 100L);

        assertEquals(7, worker.getCurrentCoordinatorId(),
                "a duplicate messageId must not overwrite the already-recorded coordinator, even with different data");
    }

    @Test
    void aGenuinelyNewMessageIdStillUpdatesAfterAnEarlierDuplicateWasIgnored() throws Exception {
        worker = new WorkerRemoteImpl(5);
        worker.receiveCoordinatorMessage(7, 100L);
        worker.receiveCoordinatorMessage(99, 100L); // ignored duplicate

        // a different messageId entirely - this is a legitimately new
        // COORDINATOR announcement (e.g. after a new term/election) and
        // should be applied normally
        worker.receiveCoordinatorMessage(3, 101L);

        assertEquals(3, worker.getCurrentCoordinatorId(),
                "a new messageId should update the coordinator normally, independent of earlier duplicates");
    }

    @Test
    void aFreshWorkerHasItsOwnIdAndNoKnownCoordinator() throws Exception {
        worker = new WorkerRemoteImpl(9);

        assertEquals(9, worker.getId(), "a worker should report the id it was constructed with");
        assertEquals(-1, worker.getCurrentCoordinatorId(),
                "a worker that has seen no COORDINATOR message must not claim to know a coordinator");

        // The election rule compares JAC values, so the starting point matters:
        // every worker must begin from the same baseline of zero assignments.
        assertEquals(0, worker.getJobAllocationCounter(),
                "a worker that has coordinated nothing should start with a Job Allocation Counter of zero");
    }

    @Test
    void aFreshWorkerReportsItselfActive() throws Exception {
        worker = new WorkerRemoteImpl(4);

        assertTrue(worker.isActive(),
                "a running worker should report itself as active so it can be discovered and handed out as a peer");
    }
}
