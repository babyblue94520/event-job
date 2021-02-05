package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import pers.clare.core.scheduler.constant.EventJobStatus;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.*;

@Log4j2
public class JdbcJobStore implements JobStore {
    private static final TypeReference<Map<String, Object>> dataType = new TypeReference<Map<String, Object>>() {
    };
    private static final int WAITING_TIME = 1000;
    private static final ObjectMapper om = new ObjectMapper();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private DataSource dataSource;

    public JdbcJobStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Boolean exists(String instance, String group, String name) throws SQLException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("select 1 from event_job where `instance` = ? and `group` = ? and `name` = ?");
            ps.setString(1, instance);
            ps.setString(2, group);
            ps.setString(3, name);
            return ps.executeQuery().next();
        } catch (Exception e) {
            throw e;
        } finally {
            close(connection);
        }
    }

    @Override
    public EventJob find(String instance, String group, String name) throws SQLException, JsonProcessingException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("select `instance`,`group`,`name`,timezone,description,concurrent,cron,prev_time,next_time,enabled,`data` from event_job where `instance` = ? and `group` = ? and `name` = ?");
            ps.setString(1, instance);
            ps.setString(2, group);
            ps.setString(3, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int index = 1;
                return new EventJob(
                        rs.getString(index++)
                        , rs.getString(index++)
                        , rs.getString(index++)
                        , rs.getString(index++)
                        , rs.getString(index++)
                        , rs.getBoolean(index++)
                        , rs.getString(index++)
                        , rs.getLong(index++)
                        , rs.getLong(index++)
                        , rs.getBoolean(index++)
                        , om.readValue(rs.getString(index++), dataType)
                );
            }
            return null;
        } catch (Exception e) {
            throw e;
        } finally {
            close(connection);
        }
    }

    @Override
    public void insert(
            String instance
            , Job job
            , long nextTime
    ) throws SQLException, JsonProcessingException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("insert into event_job(`instance`,`group`,`name`,timezone,description,concurrent,cron,next_time,enabled,`data`) values(?,?,?,?,?,?,?,?,?,?)");
            int index = 1;
            ps.setString(index++, instance);
            ps.setString(index++, job.getGroup());
            ps.setString(index++, job.getName());
            ps.setString(index++, job.getTimezone());
            ps.setString(index++, job.getDescription());
            ps.setBoolean(index++, job.getConcurrent());
            ps.setString(index++, job.getCron());
            ps.setLong(index++, nextTime);
            ps.setBoolean(index++, job.getEnabled());
            ps.setString(index++, om.writeValueAsString(job.getData()));
            ps.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            close(connection);
        }
    }

    @Override
    public void update(
            String instance
            , Job job
            , long nextTime
    ) throws SQLException, JsonProcessingException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("update event_job set timezone=?,description=?,concurrent=?,cron=?,next_time=?,enabled=? ,data=? where `instance` = ? and `group` = ? and `name` = ?");
            int index = 1;
            ps.setString(index++, job.getTimezone());
            ps.setString(index++, job.getDescription());
            ps.setBoolean(index++, job.getConcurrent());
            ps.setString(index++, job.getCron());
            ps.setLong(index++, nextTime);
            ps.setBoolean(index++, job.getEnabled());
            ps.setString(index++, om.writeValueAsString(job.getData()));
            ps.setString(index++, instance);
            ps.setString(index++, job.getGroup());
            ps.setString(index++, job.getName());
            ps.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            close(connection);
        }
    }

    @Override
    public void delete(String instance, String group, String name) throws SQLException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("delete from event_job where `instance` = ? and `group` = ? and `name` = ?");
            ps.setString(1, instance);
            ps.setString(2, group);
            ps.setString(3, name);
            ps.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            close(connection);
        }
    }


    public void executeLock(
            String instance
            , EventJob eventJob
            , Runnable runnable
    ) throws SQLException {
        if (eventJob.getConcurrent()) {
            doExecuteConcurrent(instance, eventJob, runnable);
        } else {
            doExecute(instance, eventJob, runnable);
        }
    }

    private void doExecuteConcurrent(
            String instance
            , EventJob eventJob
            , Runnable runnable
    ) throws SQLException {
        Connection connection = null;
        Boolean autocommit = null;
        Integer transactionIsolation = null;
        try {
            connection = dataSource.getConnection();
            autocommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            PreparedStatement ps;
            StringBuilder sql = new StringBuilder("update event_job set status=?,prev_time=if(prev_time<?,?,prev_time),next_time=if(next_time<?,?,next_time),runner=runner+1 where `instance` = ? and `group` = ? and `name` = ? and enabled = 1");
            ps = connection.prepareStatement(sql.toString());
            int index = 1;
            ps.setInt(index++, EventJobStatus.EXECUTING);
            ps.setLong(index++, eventJob.getPrevTime());
            ps.setLong(index++, eventJob.getPrevTime());
            ps.setLong(index++, eventJob.getNextTime());
            ps.setLong(index++, eventJob.getNextTime());
            ps.setString(index++, instance);
            ps.setString(index++, eventJob.getGroup());
            ps.setString(index++, eventJob.getName());
            int count = ps.executeUpdate();
            if (count == 0) return;
            runnable.run();
            ps = connection.prepareStatement("update event_job set runner=runner-1,status=if(runner=0,?,status) where `instance` = ? and `group` = ? and `name` = ?");
            index = 1;
            ps.setInt(index++, EventJobStatus.WAITING);
            ps.setString(index++, instance);
            ps.setString(index++, eventJob.getGroup());
            ps.setString(index++, eventJob.getName());
            ps.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            rollback(connection);
            throw e;
        } finally {
            revert(connection, autocommit, transactionIsolation);
            close(connection);
        }
    }

    private void doExecute(
            String instance
            , EventJob eventJob
            , Runnable runnable
    ) throws SQLException {
        StringBuilder sql = new StringBuilder("update event_job set status=?,prev_time=prev_time,next_time=next_time,runner=runner+1 where `instance` = ? and `group` = ? and `name` = ? and enabled = 1");
        // 非可同時執行任務，則檢查任務狀態和上次執行時間不可小於1秒內
        sql.append(" and status = ")
                .append(EventJobStatus.WAITING)
                .append(" and abs(prev_time-?)<")
                .append(WAITING_TIME)
        ;

        Connection connection = null;
        Boolean autocommit = null;
        Integer transactionIsolation = null;
        try {
            connection = dataSource.getConnection();
            autocommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement(sql.toString());
            int index = 1;
            ps.setInt(index++, EventJobStatus.EXECUTING);
            ps.setLong(index++, eventJob.getPrevTime());
            ps.setLong(index++, eventJob.getNextTime());
            ps.setString(index++, instance);
            ps.setString(index++, eventJob.getGroup());
            ps.setString(index++, eventJob.getName());
            ps.setLong(index++, eventJob.getPrevTime());
            // 確認該任務是否被鎖定中
            ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(() -> {
                try {
                    ps.cancel();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }, WAITING_TIME, TimeUnit.MILLISECONDS);
            ps.executeUpdate();
            if (scheduledFuture.cancel(true)) return;
            if (ps.getUpdateCount() == 0) return;
            runnable.run();
            PreparedStatement ps2 = connection.prepareStatement("update event_job set status=?,runner=runner-1 where `instance` = ? and `group` = ? and `name` = ?");
            index = 1;
            ps2.setInt(index++, EventJobStatus.WAITING);
            ps2.setString(index++, instance);
            ps2.setString(index++, eventJob.getGroup());
            ps2.setString(index++, eventJob.getName());

            ps2.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            rollback(connection);
            throw e;
        } finally {
            revert(connection, autocommit, transactionIsolation);
            close(connection);
        }
    }


    private void rollback(Connection connection) {
        if (connection == null) return;
        try {
            connection.rollback();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void close(Connection connection) {
        if (connection == null) return;
        try {
            connection.close();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void revert(Connection connection, Boolean autocommit, Integer transactionIsolation) {
        if (connection == null) return;
        try {
            if (autocommit != null) {
                connection.setAutoCommit(autocommit);
            }
            if (transactionIsolation != null) {
                connection.setTransactionIsolation(transactionIsolation);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }
}
