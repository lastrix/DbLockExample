package com.lastrix.dblock;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

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
                unlock();
            }
        }
    }

    protected abstract boolean tryLock();

    protected abstract void unlock();

    protected abstract AtomicInteger getLockCounter();
}
