package com.cs324.test;

import com.cs324.common.WorkerRemote;

import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Recording worker that finishes an election wave and relays coordinator
 * announcements, the way a real worker does.
 *
 * {@link RecordingWorkerRemote} answers an election by recording it and nothing
 * more, so a wave sent to one never concludes. That is what lets a test pin down
 * a moment part-way through an election - but it also means no announcement ever
 * travels anywhere, so it cannot show whether a node re-processes the broadcast
 * it just made. This double closes both gaps: it echoes its best candidate back
 * to the origin and relays announcements to its single neighbour, which makes a
 * two-node ring genuinely cyclic and the loop-back observable.
 *
 * Its JAC is settable so a test can decide which side of a wave wins instead of
 * inheriting the "all counters zero, higher id wins" default and having the
 * double lead.
 */
public class EchoingWorkerRemote extends RecordingWorkerRemote {

    private final AtomicInteger jac = new AtomicInteger(0);
    private volatile WorkerRemote relayTarget;

    public EchoingWorkerRemote(int id) throws RemoteException {
        super(id);
    }

    /** Sets the JAC this double reports and votes with. */
    public void setJobAllocationCounter(int jobs) {
        this.jac.set(jobs);
    }

    /** Sets the only neighbour this double relays announcements to. */
    public void relayAnnouncementsTo(WorkerRemote neighbour) {
        this.relayTarget = neighbour;
    }

    @Override
    public int getJobAllocationCounter() throws RemoteException {
        return jac.get();
    }

    @Override
    public void receiveElectionMessage(int originId, int candidateId, int candidateJac, long messageId)
            throws RemoteException {
        super.receiveElectionMessage(originId, candidateId, candidateJac, messageId);
        WorkerRemote origin = relayTarget;
        if (origin == null) {
            return;
        }
        // A node with no children of its own answers immediately. The candidate
        // it reports as best is its own if it beats the incoming one, otherwise
        // the incoming one - which is exactly what a real worker does, so the
        // wave's winner is decided by the JACs rather than by the test double.
        boolean weWin = jac.get() < candidateJac
                || (jac.get() == candidateJac && getId() > candidateId);
        origin.receiveEchoMessage(messageId, getId(),
                weWin ? getId() : candidateId, weWin ? jac.get() : candidateJac);
    }

    @Override
    public void receiveCoordinatorMessage(int coordinatorId, long messageId) throws RemoteException {
        super.receiveCoordinatorMessage(coordinatorId, messageId);
        // Relay to the neighbour, including the sender: a real node floods in
        // every direction, which is what lets an announcement circle back to
        // whoever broadcast it.
        WorkerRemote target = relayTarget;
        if (target != null) {
            target.receiveCoordinatorMessage(coordinatorId, messageId);
        }
    }
}
