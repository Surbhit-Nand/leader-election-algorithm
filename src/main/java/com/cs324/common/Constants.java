package com.cs324.common;

/**
 * Shared constants used across bootstrap, worker, and client modules.
 */
public class Constants {

    // Required by the assignment spec: a string variable holding "cs324"
    public static final String leaderman = "cs324";

    // Default RMI registry port; adjust as needed per node
    public static final int DEFAULT_RMI_PORT = 1099;

    // Coordinator serves for a term of 5 job assignments before re-election
    public static final int JOBS_PER_TERM = 5;

    // How often workers probe their current coordinator (heartbeat)
    public static final int HEARTBEAT_INTERVAL_MS = 2000;
}
