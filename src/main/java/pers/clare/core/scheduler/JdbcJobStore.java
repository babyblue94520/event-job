package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import pers.clare.core.scheduler.constant.EventJobStatus;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    public List<EventJob> findAll(String instance) throws SQLException, JsonProcessingException {
        List<EventJob> result = new ArrayList<>();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("select `instance`,`group`,`name`,timezone,description,cron,prev_time,next_time,enabled,`data` from event_job where `instance` = ?");
            ps.setString(1, instance);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                result.add(to(rs));
            }
            return result;
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
            PreparedStatement ps = connection.prepareStatement("select `instance`,`group`,`name`,timezone,description,cron,prev_time,next_time,enabled,`data` from event_job where `instance` = ? and `group` = ? and `name` = ?");
            ps.setString(1, instance);
            ps.setString(2, group);
            ps.setString(3, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return to(rs);
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
            PreparedStatement ps = connection.prepareStatement("insert into event_job(`instance`,`group`,`name`,timezone,description,cron,next_time,enabled,`data`) values(?,?,?,?,?,?,?,?,?)");
            int index = 1;
            ps.setString(index++, instance);
            ps.setString(index++, job.getGroup());
            ps.setString(index++, job.getName());
            ps.setString(index++, job.getTimezone());
            ps.setString(index++, job.getDescription());
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
            PreparedStatement ps = connection.prepareStatement("update event_job set timezone=?,description=?,cron=?,next_time=?,enabled=? ,data=? where `instance` = ? and `group` = ? and `name` = ?");
            int index = 1;
            ps.setString(index++, job.getTimezone());
            ps.setString(index++, job.getDescription());
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
        String group = eventJob.getGroup();
        String name = eventJob.getName();

        Connection connection = null;
        Boolean autocommit = null;
        Integer transactionIsolation = null;
        try {
            connection = dataSource.getConnection();
            transactionIsolation = connection.getTransactionIsolation();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            if (!isContinue(connection, instance, group, name)) {
                log.info("cancel");
                return;
            }
            autocommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            PreparedStatement ps = connection.prepareStatement("update event_job set status=?,prev_time=?,next_time=? where `instance` = ? and `group` = ? and `name` = ? and enabled = 1 and status = ? and prev_time<?");
            int index = 1;
            ps.setInt(index++, EventJobStatus.EXECUTING);
            ps.setLong(index++, eventJob.getPrevTime());
            ps.setLong(index++, eventJob.getNextTime());
            ps.setString(index++, instance);
            ps.setString(index++, group);
            ps.setString(index++, name);
            ps.setInt(index++, EventJobStatus.WAITING);
            ps.setLong(index++, eventJob.getPrevTime() - WAITING_TIME);
            // 確認該任務是否被鎖定中
            ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(() -> {
                try {
                    ps.cancel();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }, WAITING_TIME, TimeUnit.MILLISECONDS);
            ps.executeUpdate();
            scheduledFuture.cancel(true);
            if (!scheduledFuture.isCancelled() || ps.getUpdateCount() == 0) {
                rollback(connection);
                return;
            }
            runnable.run();
            PreparedStatement ps2 = connection.prepareStatement("update event_job set status=? where `instance` = ? and `group` = ? and `name` = ?");
            index = 1;
            ps2.setInt(index++, EventJobStatus.WAITING);
            ps2.setString(index++, instance);
            ps2.setString(index++, group);
            ps2.setString(index++, name);

            ps2.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            rollback(connection);
            log.warn(e.getMessage());
        } finally {
            revert(connection, autocommit, transactionIsolation);
            close(connection);
        }
    }

    private boolean isContinue(
            Connection connection
            , String instance
            , String group
            , String name
    ) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("select status from event_job  where `instance` = ? and `group` = ? and `name` = ?");
        int index = 1;
        ps.setString(index++, instance);
        ps.setString(index++, group);
        ps.setString(index++, name);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return Objects.equals(EventJobStatus.WAITING, rs.getInt(1));
        }
        return false;
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

    private EventJob to(ResultSet rs) throws SQLException, JsonProcessingException {
        int index = 1;
        return new EventJob(
                rs.getString(index++)
                , rs.getString(index++)
                , rs.getString(index++)
                , rs.getString(index++)
                , rs.getString(index++)
                , rs.getString(index++)
                , rs.getLong(index++)
                , rs.getLong(index++)
                , rs.getBoolean(index++)
                , om.readValue(rs.getString(index++), dataType)
        );
    }
}
