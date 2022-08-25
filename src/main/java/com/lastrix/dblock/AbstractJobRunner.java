package com.lastrix.dblock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract class for handling locking/unlocking testing
 */
public abstract class AbstractJobRunner implements Runnable {
    protected final Connection connection;
    protected final int tid;
    private final int totalLockCount;

    protected AbstractJobRunner(Connection connection, int tid, int totalLockCount) {
        this.connection = connection;
        this.tid = tid;
        this.totalLockCount = totalLockCount;
    }

    @Override
    public final void run() {
        int locked = 0;
        while (locked < totalLockCount) {
            if (tryLock()) {
                locked++;
                System.out.println("Lock acquired by thread " + Thread.currentThread().getName());
                int lockedCount = getLockCounter().incrementAndGet();
                if (lockedCount > 1) {
                    System.out.println("Locked by multiple threads! " + lockedCount);
                }
                if (unlock()) {
                    System.out.println("Released lock by " + Thread.currentThread().getName());
                } else {
                    System.out.println("Failed to unlock by " + Thread.currentThread().getName());
                }
            }
        }
    }

    protected final void rollbackSafely() {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }

    /**
     * Try to lock row in database, return true if success
     *
     * @return boolean
     */
    protected abstract boolean tryLock();

    /**
     * Try to unlock row in database, return true if success, this method should be successful all the time
     *
     * @return boolean
     */
    protected abstract boolean unlock();

    /**
     * Get current lock counter for checking that only single thread acquired our lock
     *
     * @return AtomicInteger
     */
    protected abstract AtomicInteger getLockCounter();
}
