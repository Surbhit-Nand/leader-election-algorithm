package com.cs324.worker;

import com.cs324.common.BootstrapRemote;
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
        // use to look us up again for the reverse neighbour edge.
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
            }
            // Start heartbeats: periodically ping the current coordinator via the
            // bootstrap's address records; a silent coordinator triggers a new election.
            self.setBootstrapReference(bootstrap[0], myHost);
        } catch (Exception e) {
            System.out.println("Failed to start Worker Node: " + e.getMessage());
            System.exit(1);
        }

        final WorkerRemoteImpl selfForHook = self;
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
