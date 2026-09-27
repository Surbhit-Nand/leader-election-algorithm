package com.cs324.common;

// Shared constants for the bootstrap, worker, and client modules.
public class Constants {

    // Required by the assignment spec: a string variable holding "cs324"
    public static final String leaderman = "cs324";

    // Default RMI registry port; every node in one demo must agree on it.
    // Default RMI registry port; adjust as needed per node
    public static final int DEFAULT_RMI_PORT = 1099;

    // A coordinator serves 5 client jobs per term; JAC still increments per assignment.
    // A coordinator serves a term of 5 client jobs before yielding to a new
    public static final int JOBS_PER_TERM = 5;

    // How often a worker probes the coordinator it currently believes in.
    // How often workers probe their current coordinator (heartbeat)
    public static final int HEARTBEAT_INTERVAL_MS = 2000;

    // Settle time after startup, so an already-formed cluster never holds a pointless election.
    // Grace period after startup before a worker starts an election if it has
    public static final int STARTUP_ELECTION_DELAY_MS = 3000;
}
