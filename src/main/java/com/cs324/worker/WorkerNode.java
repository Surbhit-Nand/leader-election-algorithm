package com.cs324.worker;

/**
 * Entry point for a Worker node.
 * Registers with the Bootstrap Node, connects to a random active peer,
 * and can trigger/participate in leader election and job execution.
 */
public class WorkerNode {

    public static void main(String[] args) {
        // TODO: read worker id + ports from args, register with bootstrap,
        // bind own WorkerRemoteImpl, show menu (Start Election / Show Leader / Exit)
        System.out.println("Worker Node starting...");
    }
}
