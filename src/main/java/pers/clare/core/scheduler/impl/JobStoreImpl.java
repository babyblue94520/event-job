package pers.clare.core.scheduler.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import pers.clare.core.scheduler.Job;
import pers.clare.core.scheduler.JobStore;
import pers.clare.core.scheduler.constant.EventJobStatus;
import pers.clare.core.scheduler.exception.JobException;
import pers.clare.core.scheduler.exception.JobNotExistException;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

@Log4j2
public class JobStoreImpl implements JobStore {
    private static final int WAITING_TIME = 500;

    private static final TypeReference<Map<String, Object>> dataType = new TypeReference<>() {
    };

    private static final ObjectMapper om = new ObjectMapper();

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private DataSource dataSource;

    public JobStoreImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<EventJob> findAll(String instance) throws JobException {
        List<EventJob> result = new ArrayList<>();
        Connection connection = null;
        Integer transactionIsolation = null;
        try {
            connection = dataSource.getConnection();
            transactionIsolation = connection.getTransactionIsolation();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            PreparedStatement ps = connection.prepareStatement("select `instance`,`group`,`name`,timezone,description,cron,`status`,prev_time,next_time,start_time,end_time,enabled,`data` from event_job where `instance` = ?");
            ps.setString(1, instance);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                result.add(to(rs));
            }
            return result;
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            revert(connection, null, transactionIsolation);
            close(connection);
        }
    }

    @Override
    public EventJob find(String instance, String group, String name) throws JobException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("select `instance`,`group`,`name`,timezone,description,cron,`status`,prev_time,next_time,start_time,end_time,enabled,`data` from event_job where `instance` = ? and `group` = ? and `name` = ?");
            ps.setString(1, instance);
            ps.setString(2, group);
            ps.setString(3, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return to(rs);
            }
            return null;
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void insert(
            String instance
            , Job job
            , long nextTime
    ) throws JobException {
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
            if(job.getData()==null){
                ps.setString(index++, "{}");
            }else{
                ps.setString(index++, om.writeValueAsString(job.getData()));
            }
            ps.executeUpdate();
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void update(
            String instance
            , Job job
            , long nextTime
    ) throws JobException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("update event_job set timezone=?,description=?,cron=?,next_time=?,enabled=?,`data`=? where `instance` = ? and `group` = ? and `name` = ?");
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
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void delete(String instance, String group, String name) throws JobException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("delete from event_job where `instance` = ? and `group` = ? and `name` = ?");
            ps.setString(1, instance);
            ps.setString(2, group);
            ps.setString(3, name);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public void enable(String instance, String group, String name) throws JobException {
        enabled(instance, group, name, 1);
    }

    @Override
    public void disable(String instance, String group, String name) throws JobException {
        enabled(instance, group, name, 0);
    }

    private void enabled(String instance, String group, String name, int enabled) throws JobException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("update event_job set enabled = ? where `instance` = ? and `group` = ? and `name` = ?");
            ps.setInt(1, enabled);
            ps.setString(2, instance);
            ps.setString(3, group);
            ps.setString(4, name);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    public int executor(
            EventJob eventJob
            , Runnable runnable
    ) throws JobException {
        Connection connection = null;
        Boolean autocommit = null;
        Integer transactionIsolation = null;
        try {
            connection = dataSource.getConnection();
            transactionIsolation = connection.getTransactionIsolation();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            if (!isContinue(connection, eventJob)) return 0;

            autocommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            if (compete(connection, eventJob) == 0) {
                rollback(connection);
                return 0;
            }
            runnable.run();
            eventJob.setEndTime(System.currentTimeMillis());
            finish(connection, eventJob);
            connection.commit();
            return 1;
        } catch (JobNotExistException e) {
            throw e;
        } catch (Exception e) {
            rollback(connection);
            log.error(e.getMessage(), e);
            throw new JobException(e);
        } finally {
            revert(connection, autocommit, transactionIsolation);
            close(connection);
        }
    }

    private int compete(
            Connection connection
            , EventJob eventJob
    ) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("update event_job set status=?,prev_time=?,next_time=?,start_time=?,end_time=? where `instance` = ? and `group` = ? and `name` = ? and enabled = 1 and status = ? and next_time<?");
        int index = 1;
        ps.setInt(index++, EventJobStatus.EXECUTING);
        ps.setLong(index++, eventJob.getPrevTime());
        ps.setLong(index++, eventJob.getNextTime());
        ps.setLong(index++, eventJob.getStartTime());
        ps.setLong(index++, eventJob.getEndTime());
        ps.setString(index++, eventJob.getInstance());
        ps.setString(index++, eventJob.getGroup());
        ps.setString(index++, eventJob.getName());
        ps.setInt(index++, EventJobStatus.WAITING);
        ps.setLong(index++, eventJob.getNextTime());
        ScheduledFuture scheduledFuture = executor.schedule(() -> {
            try {
                // Cancel request on timeout
                ps.cancel();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }, WAITING_TIME, TimeUnit.MILLISECONDS);
        int count = ps.executeUpdate();
        scheduledFuture.cancel(true);
        return count;
    }

    private int finish(
            Connection connection
            , EventJob eventJob
    ) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("update event_job set status=?,end_time=? where `instance` = ? and `group` = ? and `name` = ?");
        int index = 1;
        ps.setInt(index++, EventJobStatus.WAITING);
        ps.setLong(index++, eventJob.getEndTime());
        ps.setString(index++, eventJob.getInstance());
        ps.setString(index++, eventJob.getGroup());
        ps.setString(index++, eventJob.getName());
        return ps.executeUpdate();
    }

    private boolean isContinue(
            Connection connection
            , EventJob eventJob
    ) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("select status,enabled from event_job where `instance` = ? and `group` = ? and `name` = ?");
        int index = 1;
        ps.setString(index++, eventJob.getInstance());
        ps.setString(index++, eventJob.getGroup());
        ps.setString(index++, eventJob.getName());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return Objects.equals(EventJobStatus.WAITING, rs.getInt(1))
                    && Objects.equals(1, rs.getInt(2));
        }
        throw new JobNotExistException(String.format("Job(%s,%s,%s) not exist.", eventJob.getInstance(), eventJob.getGroup(), eventJob.getName()));
    }

    private void rollback(Connection connection) {
        if (connection == null) return;
        try {
            connection.rollback();
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

    private void close(Connection connection) {
        if (connection == null) return;
        try {
            connection.close();
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
                , rs.getInt(index++)
                , rs.getLong(index++)
                , rs.getLong(index++)
                , rs.getLong(index++)
                , rs.getLong(index++)
                , rs.getBoolean(index++)
                , om.readValue(rs.getString(index++), dataType)
        );
    }
}
