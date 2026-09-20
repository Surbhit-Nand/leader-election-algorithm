package com.cs324.common;

/**
 * WorkAddress
 */
public class WorkerAddress {
    String rmiHost;
    int rmiPort;

    public WorkerAddress(String Host, int Port){
        rmiHost = Host;
        rmiPort = Port;  
    }
    
    public String getRmiHost(){
        return rmiHost;
    }

    public int getRmiPort(){
        return rmiPort;
    }

}
