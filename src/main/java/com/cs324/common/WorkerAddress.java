package com.cs324.common;

/**
 * WorkAddress
 */
public class WorkerAddress implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
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
