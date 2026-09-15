package com.cs324.bootstrap;

/**
 * Entry point for the Bootstrap Node.
 * Runs as its own process. Maintains the registry of active workers.
 * Does NOT participate in leader election or job processing.
 */
public class BootstrapNode {

    public static void main(String[] args) {
        // TODO: create RMI registry, bind BootstrapRegistryImpl, print status
        System.out.println("Bootstrap Node starting...");
    }
}
