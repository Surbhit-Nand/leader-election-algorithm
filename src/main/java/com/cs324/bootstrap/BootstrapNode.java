package com.cs324.bootstrap;

import com.cs324.common.Constants;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
/**
 * Entry point for the Bootstrap Node.
 * Runs as its own process. Maintains the registry of active workers.
 * Does NOT participate in leader election or job processing.
 */
public class BootstrapNode {

    public static void main(String[] args) throws Exception {
        try {
            int port = args.length > 0 ? Integer.parseInt(args[0]) : Constants.DEFAULT_RMI_PORT;
            Registry registry = LocateRegistry.createRegistry(port);
            BootstrapRegistryImpl bootstrapRegistryImpl = new BootstrapRegistryImpl();
            registry.rebind("BootstrapRegistry", bootstrapRegistryImpl);
            System.out.println("Bootstrap Node running on port " + port + " as BootstrapRegistry");

        } catch (Exception e) {
            System.out.println("Failed to start Bootstrap Node: " + e.getMessage());
            System.exit(1);
        }
    }
}
 