package com.cs324.test;

import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Recording worker that holds one chosen job inside assignTask until released.
 *
 * RecordingWorkerRemote acknowledges an assignment immediately, which makes it
 * useless for pinning down a moment part-way through dispatch: by the time the
 * caller's submitJob returns, there is nothing left to observe. Parking a
 * specific jobId keeps the coordinator's submitJob open after it has already
 * claimed a term slot, so a test can inspect the term budget while the last job
 * of that term is still going out - which is exactly the window in which an
 * unsynchronised counter would let a sixth job through.
 *
 * Like its parent it never reports a result, so no job it is handed ever
 * completes and the term cannot rotate out from under the test.
 */
public class BlockingAssignWorkerRemote extends RecordingWorkerRemote {

    private static final long BLOCK_TIMEOUT_SECONDS = 30L;

    private volatile int blockJobId = Integer.MIN_VALUE;
    private volatile CountDownLatch gate = new CountDownLatch(0);

    public BlockingAssignWorkerRemote(int id) throws RemoteException {
        super(id);
    }

    /** Parks assignTask for this jobId until release() is called. */
    public void blockJob(int jobId) {
        this.blockJobId = jobId;
        this.gate = new CountDownLatch(1);
    }

    /** Lets a blocked assignTask return. Safe to call when nothing is blocked. */
    public void release() {
        gate.countDown();
    }

    @Override
    public void assignTask(int jobId, String jobType, Object payload) throws RemoteException {
        super.assignTask(jobId, jobType, payload);
        if (jobId != blockJobId) {
            return;
        }
        try {
            // A timeout rather than an unbounded wait: a failing test must not
            // hang the build, and the latch is released explicitly in the happy path.
            gate.await(BLOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
