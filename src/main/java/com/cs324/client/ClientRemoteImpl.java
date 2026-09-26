package com.cs324.client;

import com.cs324.common.ClientRemote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Lives inside the Client process and receives the final aggregated result of
 * a job once the coordinator has collected every sub-result.
 */
public class ClientRemoteImpl extends UnicastRemoteObject implements ClientRemote {

    private final ResultHandler handler;

    public interface ResultHandler {
        void handle(int jobId, Object result);
    }

    public ClientRemoteImpl(ResultHandler handler) throws RemoteException {
        this.handler = handler;
    }

    @Override
    public void receiveJobResult(int jobId, Object result) throws RemoteException {
        handler.handle(jobId, result);
    }
}