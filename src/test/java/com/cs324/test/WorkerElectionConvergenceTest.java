package com.cs324.test;

import com.cs324.worker.WorkerRemoteImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Step 5 of the worker test plan: the election end to end.
 *
 * Everything before this tests one hop. This drives a real multi-hop election
 * across a whole worker graph in the same JVM, talking over RMI, and checks the
 * two things the assignment actually asks for:
 *
 *   - every reachable worker agrees on ONE coordinator, and
 *   - the winner is lowest JAC, with ties broken by highest worker id.
 *
 * The triangle topology matters. In a triangle two of the nodes forward the
 * ELECTION to EACH OTHER, so each receives a duplicate. If a duplicate were
 * silently dropped instead of answered, those two would wait for each other
 * forever. The timeout inside awaitAgreement() is what turns that deadlock into
 * a clear failure instead of a hung test run.
 *
 * The same properties are then re-checked on a five-worker graph, which puts
 * the cycle and a two-worker branch on the same node. See
 * {@link #fullElectionConvergesAcrossMixedFiveWorkerTopology()}.
 */
class WorkerElectionConvergenceTest {

    private static final int TEST_RMI_PORT = 19804;
    private static final String HOST = "localhost";
    private static final long CONVERGENCE_TIMEOUT_MS = 5_000;

    private static Registry registry;

    private WorkerRemoteImpl w1;
    private WorkerRemoteImpl w2;
    private WorkerRemoteImpl w3;
    private WorkerRemoteImpl w4;
    private WorkerRemoteImpl w5;

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
        w4 = new WorkerRemoteImpl(4);
        w5 = new WorkerRemoteImpl(5);

        registry.rebind("Worker1", w1);
        registry.rebind("Worker2", w2);
        registry.rebind("Worker3", w3);
        registry.rebind("Worker4", w4);
        registry.rebind("Worker5", w5);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (String name : registry.list()) {
            registry.unbind(name);
        }
        unexportQuietly(w1);
        unexportQuietly(w2);
        unexportQuietly(w3);
        unexportQuietly(w4);
        unexportQuietly(w5);
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

    private WorkerRemoteImpl worker(int id) {
        return switch (id) {
            case 1 -> w1;
            case 2 -> w2;
            case 3 -> w3;
            case 4 -> w4;
            case 5 -> w5;
            default -> throw new IllegalArgumentException("unknown worker " + id);
        };
    }

    private void connect(int fromId, int toId) throws Exception {
        worker(fromId).connectAsNeighbour(toId, HOST, TEST_RMI_PORT);
    }

    /** 1-2, 2-3, 3-1: every node has two neighbours, and the graph contains a cycle. */
    private void wireTriangle() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(2, 3);
        connect(3, 2);
        connect(3, 1);
        connect(1, 3);
    }

    /** 1-2-3: a plain line, with no cycle at all. */
    private void wireChain() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(2, 3);
        connect(3, 2);
    }

    /**
     * A triangle on 1-2-3 with a two-worker tail hanging off worker 3:
     * 1<->2, 1<->3, 2<->3, 3<->4, 4<->5.
     *
     * This is the only shape in the file where the cycle and the dead ends sit
     * on opposite sides of the same node. Worker 3 forwards the election both
     * backwards into the triangle and forwards into the tail, so the winning
     * candidate has to be carried into a branch whose members are the strongest
     * in the graph - and the two rules (lowest JAC first, highest id second) have
     * to be applied to candidates that never met each other.
     */
    private void wireTriangleWithTwoWorkerTail() throws Exception {
        connect(1, 2);
        connect(2, 1);
        connect(1, 3);
        connect(3, 1);
        connect(2, 3);
        connect(3, 2);
        connect(3, 4);
        connect(4, 3);
        connect(4, 5);
        connect(5, 4);
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

    /** Polls the three workers wired by the triangle/chain helpers. */
    private void awaitAgreement(int expectedCoordinatorId) throws Exception {
        awaitAgreement(expectedCoordinatorId, w1, w2, w3);
    }

    /**
     * Polls every supplied worker until they all report the expected
     * coordinator.
     *
     * The participating set is a parameter rather than a fixed trio so a
     * topology of any size can reuse the same convergence check. Voting is
     * asynchronous, so a single read can catch a worker mid-flight; keep
     * polling until the deadline instead of reading once and trusting it.
     */
    private void awaitAgreement(int expectedCoordinatorId, WorkerRemoteImpl... participants) throws Exception {
        long deadline = System.currentTimeMillis() + CONVERGENCE_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            boolean agreed = true;
            for (WorkerRemoteImpl participant : participants) {
                if (participant.getCurrentCoordinatorId() != expectedCoordinatorId) {
                    agreed = false;
                    break;
                }
            }
            if (agreed) {
                return;
            }
            Thread.sleep(20);
        }

        fail("the election did not converge on coordinator " + expectedCoordinatorId + " within "
                + CONVERGENCE_TIMEOUT_MS + "ms - if the workers are stuck at -1 or disagree, the echo phase never "
                + "terminated (a duplicate that was dropped instead of answered does exactly this). Got: "
                + describe(participants));
    }

    /** Renders "w1=3, w2=5, ..." for the failure message. */
    private String describe(WorkerRemoteImpl... participants) throws RemoteException {
        StringBuilder rendered = new StringBuilder();
        for (WorkerRemoteImpl participant : participants) {
            if (rendered.length() > 0) {
                rendered.append(", ");
            }
            rendered.append("w").append(participant.getId())
                    .append("=").append(participant.getCurrentCoordinatorId());
        }
        return rendered.toString();
    }

    @Test
    void aTriangleConvergesOnOneCoordinatorAndTiesGoToTheHighestId() throws Exception {
        wireTriangle();

        w1.startElection();

        // All three start on JAC 0, so the id tie-break decides: worker 3 wins.
        awaitAgreement(3);
    }

    @Test
    void theWorkerWithTheLowestJobAllocationCounterWins() throws Exception {
        wireTriangle();
        seedJobAllocationCounter(w1, 0);
        seedJobAllocationCounter(w2, 5);
        seedJobAllocationCounter(w3, 9);

        // Worker 3 has the highest id, but the lowest JAC must take priority.
        w2.startElection();

        awaitAgreement(1);
    }

    @Test
    void whenLowestJacIsTiedTheHigherWorkerIdWins() throws Exception {
        wireTriangle();
        seedJobAllocationCounter(w1, 2);
        seedJobAllocationCounter(w2, 1);
        seedJobAllocationCounter(w3, 1);

        w1.startElection();

        // Workers 2 and 3 share the lowest JAC, so the election has to settle
        // that tie the same way it always does - on the higher id. This is the
        // only test where the two rules are applied in sequence on the same
        // election: the JAC decides which ids are even in the running, then the
        // id picks the winner among them.
        awaitAgreement(3);
    }

    @Test
    void aChainTopologyWithNoCycleAlsoConverges() throws Exception {
        wireChain();

        w2.startElection();

        awaitAgreement(3);
    }

    @Test
    void aSecondElectionIsNotMistakenForTheFirst() throws Exception {
        wireTriangle();

        w1.startElection();
        awaitAgreement(3);

        // A fresh election must be processed on its own merits, not treated as a
        // duplicate of the one that just finished.
        w1.startElection();
        awaitAgreement(3);

        assertEquals(3, w2.getCurrentCoordinatorId(),
                "a new election must be applied normally rather than discarded as a repeat");
    }

    @Test
    void fullElectionConvergesAcrossMixedFiveWorkerTopology() throws Exception {
        wireTriangleWithTwoWorkerTail();

        seedJobAllocationCounter(w1, 4);
        seedJobAllocationCounter(w2, 2);
        seedJobAllocationCounter(w3, 3);
        seedJobAllocationCounter(w4, 1);
        seedJobAllocationCounter(w5, 1);

        // Worker 2 is neither the origin of the graph nor anywhere near the
        // winner, so the ELECTION has to travel the triangle and then out along
        // the tail before the winner is even known.
        w2.startElection();

        // Workers 4 and 5 share the lowest JAC in the graph at 1, so the JAC
        // rule eliminates workers 1, 2 and 3 outright, and the id rule then
        // picks 5 over 4. Crucially, worker 5 is a leaf on the far side of the
        // graph: its own JAC is the only thing that puts it ahead of a candidate
        // that never reaches it directly, so a propagation bug that stops at the
        // branch point (worker 3) shows up as worker 5 never winning.
        awaitAgreement(5, w1, w2, w3, w4, w5);
    }
}
