package com.lastrix.dblock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class UpdateLockTest {
    private static final int TOTAL_LOCK_COUNT = 10;
    private static final AtomicInteger LOCK_COUNT = new AtomicInteger(0);

    public static void main(String[] args) {
        new AbstractDBTest() {
            @Override
            protected void setup(Connection connection) throws SQLException {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("CREATE SCHEMA IF NOT EXISTS db_locks;");
                    stmt.execute("DROP TABLE IF EXISTS db_locks.update_lock;");
                    stmt.execute("CREATE TABLE db_locks.update_lock(" +
                            "id INT NOT NULL PRIMARY KEY," +
                            "state INT NOT NULL DEFAULT 0" +
                            ")");
                    stmt.execute("INSERT INTO db_locks.update_lock(id) VALUES(1)");
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
        private int id;

        public JobRunner(Connection connection, int tid) {
            super(connection, tid, TOTAL_LOCK_COUNT);
        }

        @Override
        protected boolean tryLock() {
            try (var selStmt = connection.prepareStatement("SELECT id FROM db_locks.update_lock t WHERE t.state = 0 ORDER BY id LIMIT 1;");
                 var updStmt = connection.prepareStatement("UPDATE db_locks.update_lock t SET state = 1 WHERE t.id = ? AND t.state = 0")
            ) {
                try (var rs = selStmt.executeQuery()) {
                    if (rs.next()) {
                        id = rs.getInt(1);
                    } else {
                        rollbackSafely();
                        return false;
                    }
                }
                updStmt.setInt(1, id);
                boolean success = updStmt.executeUpdate() > 0;
                connection.commit();
                return success;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected boolean unlock() {
            try (var stmt = connection.prepareStatement("UPDATE db_locks.update_lock t SET state = 0 WHERE t.id = ? AND t.state = 1")) {
                stmt.setInt(1, id);
                if ( stmt.executeUpdate() == 0) {
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
            return LOCK_COUNT;
        }
    }
}
