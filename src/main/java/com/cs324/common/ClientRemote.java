package com.cs324.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI contract implemented by a Client node.
 * The coordinator calls back here with the final aggregated result of a job.
 */
public interface ClientRemote extends Remote {

    void receiveJobResult(int jobId, Object result) throws RemoteException;
}