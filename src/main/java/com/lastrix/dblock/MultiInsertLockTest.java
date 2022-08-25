package com.lastrix.dblock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiInsertLockTest {
    private static final int TOTAL_LOCK_COUNT = 10;
    private static final AtomicInteger LOCK_COUNT_EVEN = new AtomicInteger(0);
    private static final AtomicInteger LOCK_COUNT_ODD = new AtomicInteger(0);

    public static void main(String[] args) {
        new AbstractDBTest() {
            @Override
            protected void setup(Connection connection) throws SQLException {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("CREATE SCHEMA IF NOT EXISTS db_locks;");
                    stmt.execute("CREATE TABLE IF NOT EXISTS db_locks.multi_insert_lock(" +
                            "id INT NOT NULL PRIMARY KEY" +
                            ")");
                }
                connection.commit();
            }

            @Override
            protected void runJob(Connection connection, int tid) {
                new JobRunner(connection, tid).run();
            }
        }.run();

    }

    private static class JobRunner extends AbstractJobRunner {
        private final int id;

        public JobRunner(Connection connection, int tid) {
            super(connection, tid, TOTAL_LOCK_COUNT);
            id = 1;
        }

        @Override
        protected boolean tryLock() {
            try (var stmt = connection.prepareStatement("INSERT INTO db_locks.multi_insert_lock(id) VALUES(?)")) {
                stmt.setInt(1, id);
                boolean success = stmt.executeUpdate() > 0;
                connection.commit();
                return success;
            } catch (SQLException e) {
                rollbackSafely();
                if (isValidFailure(e)) {
                    return false;
                }
                throw new RuntimeException(e);
            }
        }

        @Override
        protected boolean unlock() {
            try (var stmt = connection.prepareStatement("DELETE FROM db_locks.multi_insert_lock t WHERE t.id = ?")) {
                stmt.setInt(1, id);
                if (stmt.executeUpdate() == 0) {
                    rollbackSafely();
                    return false;
                }
                // we need to do this before commit, otherwise race-condition may occur
                getLockCounter().decrementAndGet();
                connection.commit();
                return true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected AtomicInteger getLockCounter() {
            return tid % 2 == 0 ? LOCK_COUNT_EVEN : LOCK_COUNT_ODD;
        }

        private static boolean isValidFailure(SQLException e) {
            if (e.getMessage().contains("duplicate key value violates unique constraint")) {
                return true;
            }
            return e.getMessage().contains("current transaction is aborted, commands ignored until end of transaction block");
        }
    }
}
