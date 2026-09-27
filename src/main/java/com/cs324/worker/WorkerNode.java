package com.cs324.worker;

import com.cs324.common.BootstrapRemote;
import com.cs324.common.Constants;
import com.cs324.common.WorkerAddress;
import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

/**
 * Entry point for a Worker node.
 * Registers with the Bootstrap Node, connects to a random active peer,
 * and can trigger/participate in leader election and job execution.
 *
 * Usage: WorkerNode &lt;workerId&gt; &lt;rmiPort&gt; &lt;bootstrapHost&gt; &lt;bootstrapPort&gt;
 */
public class WorkerNode {

    /** Required by the assignment spec: worker nodes hold a "cs324" marker. */
    public static final String leaderman = "cs324";

    // Low half of a locally generated coordinator-announcement id. A value no
    // election counter reaches, so adopting a peer's leader can never be
    // mistaken for a repeat of an election message.
    private static final long ADOPT_MESSAGE_TAG = 0x5AD0L;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: WorkerNode <workerId> <rmiPort> <bootstrapHost> <bootstrapPort>");
            System.exit(1);
        }

        int workerId;
        int rmiPort;
        int bootstrapPort;
        String bootstrapHost = args[2];
        try {
            workerId = Integer.parseInt(args[0]);
            rmiPort = Integer.parseInt(args[1]);
            bootstrapPort = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("Worker id and ports must be integers: " + e.getMessage());
            System.exit(1);
            return;
        }

        // This demo runs on localhost; the address we advertise is what peers
        // use to look us up again for the reverse neighbour edge. For a
        // multi-host run only this line changes: pass the real hostname so
        // remote workers register a reachable address.
        String myHost = "localhost";

        WorkerRemoteImpl self = null;
        Registry localRegistry = null;
        final BootstrapRemote[] bootstrap = new BootstrapRemote[1];
        final int idForHook = workerId;
        try {
            localRegistry = LocateRegistry.createRegistry(rmiPort);
            self = new WorkerRemoteImpl(workerId);
            localRegistry.rebind("Worker" + workerId, self);
            System.out.println("Worker " + workerId + " bound as Worker" + workerId + " on port " + rmiPort);

            Registry bootstrapRegistry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            bootstrap[0] = (BootstrapRemote) bootstrapRegistry.lookup("BootstrapRegistry");

            WorkerRemote peerStub = bootstrap[0].registerWorker(workerId, myHost, rmiPort);
            if (peerStub == null) {
                System.out.println("First worker in the network - no peer to connect to.");
            } else {
                int peerId = peerStub.getId();
                WorkerAddress peerAddress = bootstrap[0].getWorkerAddress(peerId);
                String peerHost = peerAddress != null ? peerAddress.getRmiHost() : bootstrapHost;
                int peerPort = peerAddress != null ? peerAddress.getRmiPort() : bootstrapPort;

                // Forward edge: remember the peer locally.
                self.connectAsNeighbour(peerId, peerHost, peerPort);
                // Reverse edge: ask the peer to remember us (bidirectional link).
                // The successful RMI call is the acknowledgement.
                try {
                    peerStub.connectAsNeighbour(workerId, myHost, rmiPort);
                    System.out.println("Connected to peer worker " + peerId + " (bidirectional).");
                } catch (Exception e) {
                    System.out.println("Peer found but reverse connect failed: " + e.getMessage());
                }
                // Adopt the peer's coordinator if the cluster already has one,
                // so a late joiner neither waits for an announcement it missed
                // nor starts an election the cluster does not need.
                try {
                    int peerCoordinator = peerStub.getCurrentCoordinatorId();
                    if (peerCoordinator != -1) {
                        // Locally generated id: this node has seen no announcements
                        // yet, so it cannot collide with one the cluster already sent.
                        // Adopted without relaying - the peer already knows this
                        // coordinator, and re-broadcasting it would reach the
                        // coordinator under a fresh id, which it would read as a
                        // new term and answer with a second full job budget.
                        self.adoptCoordinator(peerCoordinator, adoptMessageId(workerId));
                        System.out.println("Adopted coordinator " + peerCoordinator
                                + " from peer worker " + peerId + ".");
                    }
                } catch (Exception e) {
                    System.out.println("Could not read the peer's coordinator: " + e.getMessage());
                }
            }
            // Start heartbeats: periodically ping the current coordinator via the
            // bootstrap's address records; a silent coordinator triggers a new election.
            self.setBootstrapReference(bootstrap[0], myHost);
        } catch (Exception e) {
            System.out.println("Failed to start Worker Node: " + e.getMessage());
            System.exit(1);
        }

        final WorkerRemoteImpl selfForHook = self;
        final WorkerRemoteImpl selfForStartup = self;
        if (selfForStartup != null) {
            // The cluster elects itself: after a grace period, a worker that
            // still knows no coordinator starts a wave. The delay lets the rest
            // of the cluster register and adopt the first leader, so a normal
            // startup produces one election rather than one per worker. A node
            // that already adopted or received a coordinator stays quiet.
            Thread startupElection = new Thread(() -> {
                try {
                    Thread.sleep(Constants.STARTUP_ELECTION_DELAY_MS);
                    if (selfForStartup.getCurrentCoordinatorId() == -1) {
                        System.out.println("No coordinator known after startup - starting an election.");
                        selfForStartup.startElection();
                    } else {
                        System.out.println("Coordinator " + selfForStartup.getCurrentCoordinatorId()
                                + " already known - no startup election needed.");
                    }
                } catch (Exception e) {
                    System.out.println("Startup election skipped: " + e.getMessage());
                }
            }, "worker-startup-election-" + workerId);
            // Daemon: must never hold the JVM open on its own.
            startupElection.setDaemon(true);
            startupElection.start();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                // Stop the job thread pool first. If removed, pool threads leak and the JVM can hang on exit.
                if (selfForHook != null) {
                    selfForHook.shutdown();
                }
            } catch (Exception ignored) {
                // best effort on shutdown
            }
            try {
                if (bootstrap[0] != null) {
                    bootstrap[0].deregisterWorker(idForHook);
                }
            } catch (Exception ignored) {
                // best effort on shutdown
            }
            try {
                if (selfForHook != null) {
                    java.rmi.server.UnicastRemoteObject.unexportObject(selfForHook, true);
                }
            } catch (Exception ignored) {
                // already unexported
            }
        }));

        runMenu(self, workerId);

        // RMI export keeps non-daemon threads alive, so a plain return would
        // leave the JVM running forever. Explicit exit runs the shutdown hook,
        // which deregisters from the Bootstrap and unexports the remote object.
        System.exit(0);
    }

    /**
     * Message id for a locally adopted coordinator announcement. Only this
     * node's own id space matters: it is used to seed a joining worker's
     * duplicate-suppression set, and elections use their own ids.
     */
    private static long adoptMessageId(int workerId) {
        return ((long) workerId << 32) | ADOPT_MESSAGE_TAG;
    }

    private static void runMenu(WorkerRemoteImpl self, int workerId) {
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                int coordinatorId;
                try {
                    coordinatorId = self.getCurrentCoordinatorId();
                } catch (RemoteException e) {
                    coordinatorId = -1;
                }
                System.out.println();
                System.out.println("Worker " + workerId + " | coordinator=" + coordinatorId
                        + " | neighbours=" + self.getNeighbourIds());
                System.out.println("[1] Start Election  [2] Show Leader  [3] Exit");
                System.out.print("> ");
                if (!scanner.hasNextLine()) {
                    return;
                }
                String choice = scanner.nextLine().trim();
                switch (choice) {
                    case "1" -> {
                        long electionId = self.startElection();
                        System.out.println("Election started (messageId=" + electionId + ").");
                    }
                    case "2" -> System.out.println("Current coordinator: " + coordinatorId);
                    case "3" -> {
                        System.out.println("Leaving - deregistering from Bootstrap.");
                        return;
                    }
                    default -> System.out.println("Unknown option: " + choice);
                }
            }
        }
    }
}
