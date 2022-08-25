package com.lastrix.dblock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractDBTest {
    public static final int THREAD_COUNT = 8;

    private final int threadCount;

    protected AbstractDBTest() {
        this(THREAD_COUNT);
    }

    protected AbstractDBTest(int threadCount) {
        this.threadCount = threadCount;
    }

    public void run(){
        try (var connection = createConnection()) {
            setup(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        var start = Instant.now();
        doTest();
        System.out.println("Time taken = " + Duration.between(start, Instant.now()).toMillis() + " ms");
    }

    private void doTest() {
        CountDownLatch latch = new CountDownLatch(threadCount);
        var complete = new CountDownLatch(threadCount);
        var runnable = new Runnable() {
            @Override
            public void run() {
                try (var connection = createConnection()) {
                    latch.countDown();
                    latch.await();
                    runJob(connection, getThreadId());
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                } finally {
                    complete.countDown();
                }
            }
        };

        for (int i = 0; i < threadCount; i++) {
            new Thread(runnable, "test-thread-" + i).start();
        }

        try {
            complete.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getThreadId() {
        var n = Thread.currentThread().getName();
        var idx = n.lastIndexOf('-');
        return Integer.parseInt(n.substring(idx + 1));
    }

    private static Connection createConnection() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "test", "test2002");
        connection.setAutoCommit(false);
        return connection;
    }

    protected abstract void setup(Connection connection) throws SQLException;

    protected abstract void runJob(Connection connection, int tid) throws Exception;
}
