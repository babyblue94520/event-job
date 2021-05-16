package pers.clare.eventjob.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pers.clare.eventjob.Job;
import pers.clare.eventjob.JobStore;
import pers.clare.eventjob.constant.EventJobStatus;
import pers.clare.eventjob.exception.JobException;
import pers.clare.eventjob.exception.JobNotExistException;
import pers.clare.eventjob.function.JobExecutor;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class JdbcJobStoreImpl implements JobStore {
    private static final Logger log = LogManager.getLogger();

    private static final int WAITING_TIME = 500;

    private static final TypeReference<Map<String, Object>> dataType = new TypeReference<Map<String, Object>>() {
    };

    private static final ObjectMapper om = new ObjectMapper();

    private static final String findAll = "select `instance`,`group`,`name`,event,timezone,description,cron,`status`,prev_time,next_time,start_time,end_time,enabled,`data` from event_job where `instance` = ?";

    private static final String findAllByGroup = "select `instance`,`group`,`name`,event,timezone,description,cron,`status`,prev_time,next_time,start_time,end_time,enabled,`data` from event_job where `instance` = ? and group = ?";

    private static final String find = "select `instance`,`group`,`name`,event,timezone,description,cron,`status`,prev_time,next_time,start_time,end_time,enabled,`data` from event_job where `instance` = ? and `group` = ? and `name` = ?";

    private static final String findStatus = "select status,enabled from event_job where `instance` = ? and `group` = ? and `name` = ?";

    private static final String insert = "insert into event_job(`instance`,`group`,`name`,event,timezone,description,cron,next_time,enabled,`data`) values(?,?,?,?,?,?,?,?,?,?)";

    private static final String update = "update event_job set event=?,timezone=?,description=?,cron=?,next_time=?,enabled=?,`data`=? where `instance` = ? and `group` = ? and `name` = ?";

    private static final String updateExecuting = "update event_job set status=?,prev_time=?,next_time=?,start_time=?,end_time=? where `instance` = ? and `group` = ? and `name` = ? and enabled = 1 and status = ? and next_time<?";

    private static final String updateExecuted = "update event_job set status=?,end_time=? where `instance` = ? and `group` = ? and `name` = ?";

    private static final String updateEnabledByGroup = "update event_job set enabled = ? where `instance` = ? and `group` = ?";

    private static final String updateEnabled = "update event_job set enabled = ? where `instance` = ? and `group` = ? and `name` = ?";

    private static final String deleteByGroup = "delete from event_job where `instance` = ? and `group` = ?";

    private static final String delete = "delete from event_job where `instance` = ? and `group` = ? and `name` = ?";

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private final DataSource dataSource;

    public JdbcJobStoreImpl(DataSource dataSource) {
        this.dataSource = dataSource;
        init();
    }

    private void init() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            Statement statement = connection.createStatement();
            String schema = getInitSchema(databaseMetaData.getDatabaseProductName());
            if (schema == null) return;
            String[] commands = schema.split(";");
            for (String command : commands) {
                statement.executeUpdate(command);
            }
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    private String getInitSchema(String database) {
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(database + ".sql")) {
            if (inputStream != null) {
                return new BufferedReader(new InputStreamReader(inputStream))
                        .lines().collect(Collectors.joining("\n"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
            PreparedStatement ps = connection.prepareStatement(findAll);
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
    public List<EventJob> findAll(String instance, String group) throws JobException {
        List<EventJob> result = new ArrayList<>();
        Connection connection = null;
        Integer transactionIsolation = null;
        try {
            connection = dataSource.getConnection();
            transactionIsolation = connection.getTransactionIsolation();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            PreparedStatement ps = connection.prepareStatement(findAllByGroup);
            ps.setString(1, instance);
            ps.setString(2, group);
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
            PreparedStatement ps = connection.prepareStatement(find);
            setValue(ps, instance, group, name);
            ResultSet rs = ps.executeQuery();
            return rs.next() ? to(rs) : null;
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
        try {
            String data = job.getData() == null ? "{}" : om.writeValueAsString(job.getData());
            executeUpdate(insert
                    , instance
                    , job.getGroup()
                    , job.getName()
                    , job.getEvent()
                    , job.getTimezone()
                    , job.getDescription()
                    , job.getCron()
                    , nextTime
                    , job.getEnabled()
                    , data
            );
        } catch (JobException e) {
            throw e;
        } catch (Exception e) {
            throw new JobException(e);
        }
    }

    @Override
    public void update(
            String instance
            , Job job
            , long nextTime
    ) throws JobException {
        try {
            String data = job.getData() == null ? "{}" : om.writeValueAsString(job.getData());
            executeUpdate(update
                    , job.getEvent()
                    , job.getTimezone()
                    , job.getDescription()
                    , job.getCron()
                    , nextTime
                    , job.getEnabled()
                    , data
                    , instance
                    , job.getGroup()
                    , job.getName()
            );
        } catch (JobException e) {
            throw e;
        } catch (Exception e) {
            throw new JobException(e);
        }
    }

    @Override
    public void delete(String instance, String group) throws JobException {
        executeUpdate(deleteByGroup, instance, group);
    }

    @Override
    public void delete(String instance, String group, String name) throws JobException {
        executeUpdate(delete, instance, group, name);
    }

    @Override
    public void enable(String instance, String group) throws JobException {
        executeUpdate(updateEnabledByGroup, 1, instance, group);
    }

    @Override
    public void enable(String instance, String group, String name) throws JobException {
        executeUpdate(updateEnabled, 1, instance, group, name);
    }

    @Override
    public void disable(String instance, String group) throws JobException {
        executeUpdate(updateEnabledByGroup, 0, instance, group);
    }

    @Override
    public void disable(String instance, String group, String name) throws JobException {
        executeUpdate(updateEnabled, 0, instance, group, name);
    }

    public int executor(
            EventJob eventJob
            , JobExecutor executor
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
            executor.execute(eventJob);
            eventJob.setEndTime(System.currentTimeMillis());
            int count = finish(connection, eventJob);
            connection.commit();
            return count;
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
        PreparedStatement ps = connection.prepareStatement(updateExecuting);
        setValue(ps, EventJobStatus.EXECUTING, eventJob.getPrevTime(), eventJob.getNextTime(), eventJob.getStartTime(), eventJob.getEndTime(), eventJob.getInstance(), eventJob.getGroup(), eventJob.getName(), EventJobStatus.WAITING, eventJob.getNextTime());
        ScheduledFuture<?> scheduledFuture = executor.schedule(() -> {
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
        PreparedStatement ps = connection.prepareStatement(updateExecuted);
        setValue(ps, EventJobStatus.WAITING, eventJob.getEndTime(), eventJob.getInstance(), eventJob.getGroup(), eventJob.getName());
        return ps.executeUpdate();
    }

    private boolean isContinue(
            Connection connection
            , EventJob eventJob
    ) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(findStatus);
        setValue(ps, eventJob.getInstance(), eventJob.getGroup(), eventJob.getName());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            return Objects.equals(EventJobStatus.WAITING, rs.getInt(1))
                    && Objects.equals(1, rs.getInt(2));
        }
        throw new JobNotExistException(String.format("Job(%s,%s,%s) not exist.", eventJob.getInstance(), eventJob.getGroup(), eventJob.getName()));
    }

    private void executeUpdate(String sql, Object... parameters) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            setValue(ps, parameters);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    private void setValue(PreparedStatement ps, Object... parameters) throws SQLException {
        int index = 1;
        for (Object parameter : parameters) {
            ps.setObject(index++, parameter);
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
                , rs.getString(index++)
                , rs.getInt(index++)
                , rs.getLong(index++)
                , rs.getLong(index++)
                , rs.getLong(index++)
                , rs.getLong(index++)
                , rs.getBoolean(index++)
                , om.readValue(rs.getString(index), dataType)
        );
    }
}
