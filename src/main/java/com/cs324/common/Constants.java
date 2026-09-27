package com.cs324.common;

/**
 * Shared constants used across bootstrap, worker, and client modules.
 */
public class Constants {

    // Required by the assignment spec: a string variable holding "cs324"
    public static final String leaderman = "cs324";

    // Default RMI registry port; adjust as needed per node
    public static final int DEFAULT_RMI_PORT = 1099;

    // A coordinator serves a term of 5 client jobs before yielding to a new
    // election. The term counts jobs, not task assignments: the Job Allocation
    // Counter still increments once per assignment (see WorkerRemoteImpl) and
    // is only the metric candidates are compared on.
    public static final int JOBS_PER_TERM = 5;

    // How often workers probe their current coordinator (heartbeat)
    public static final int HEARTBEAT_INTERVAL_MS = 2000;

    // Grace period after startup before a worker starts an election if it has
    // not heard of a coordinator. Long enough for the rest of the cluster to
    // register and adopt a leader, so only a genuinely leaderless cluster votes.
    public static final int STARTUP_ELECTION_DELAY_MS = 3000;
}
