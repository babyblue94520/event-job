package pers.clare.eventjob.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import pers.clare.eventjob.vo.DependentJob;
import pers.clare.eventjob.vo.EventJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import pers.clare.eventjob.JobStatus;
import pers.clare.eventjob.JobStore;
import pers.clare.eventjob.constant.EventJobStatus;
import pers.clare.eventjob.exception.JobException;
import pers.clare.eventjob.util.DataSourceSchemaUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JdbcJobStoreImpl implements JobStore, InitializingBean {
    private static final Logger log = LogManager.getLogger();

    private static final TypeReference<Map<String, Object>> dataType = new TypeReference<>() {
    };

    private static final ObjectMapper om = new ObjectMapper();

    private static final String findAll = "select `group`,`name`,event,timezone,description,cron,after_group,after_name,enabled,`data` from event_job where `instance` = ?";

    private static final String findAllByGroup = "select `group`,`name`,event,timezone,description,cron,after_group,after_name,enabled,`data` from event_job where `instance` = ? and `group` = ?";

    private static final String find = "select `group`,`name`,event,timezone,description,cron,after_group,after_name,enabled,`data` from event_job where `instance` = ? and `group` = ? and `name` = ?";

    private static final String findStatus = "select status, next_time, enabled from event_job where `instance` = ? and `group` = ? and `name` = ?";

    private static final String findDependentJob = "select cron,timezone,after_group,after_name from event_job where `instance` = ? and `group` = ? and `name` = ?";

    private static final String insert = "insert into event_job(`instance`,`group`,`name`,event,timezone,description,cron,after_group,after_name,next_time,enabled,`data`) values(?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String update = "update event_job set event=?,timezone=?,description=?,cron=?,next_time=?,enabled=?,`data`=? where `instance` = ? and `group` = ? and `name` = ?";

    private static final String updateRelease = "update event_job set status=? where `instance` = ? and `group` = ? and `name` = ? and status = ? and next_time<?";

    private static final String updateExecuting = "update event_job set status=?,prev_time=start_time,next_time=?,start_time=?,end_time=0 where `instance` = ? and `group` = ? and `name` = ? and enabled = 1 and status = ? and next_time<?";

    private static final String updateExecuted = "update event_job set status=?,end_time=? where `instance` = ? and `group` = ? and `name` = ?";

    private static final String updateEnabledByGroup = "update event_job set enabled = ? where `instance` = ? and `group` = ?";

    private static final String updateEnabled = "update event_job set enabled = ? where `instance` = ? and `group` = ? and `name` = ?";

    private static final String deleteByGroup = "delete from event_job where `instance` = ? and `group` = ?";

    private static final String delete = "delete from event_job where `instance` = ? and `group` = ? and `name` = ?";

    private final DataSource dataSource;

    public JdbcJobStoreImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            DataSourceSchemaUtil.init(dataSource);
        } catch (SQLException e) {
            log.error(e);
        }
    }

    @Override
    public List<EventJob> findAll(String instance) throws JobException {
        if (instance == null) return Collections.emptyList();
        List<EventJob> result = new ArrayList<>();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
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
            close(connection);
        }
    }

    @Override
    public List<EventJob> findAll(String instance, String group) throws JobException {
        if (instance == null || group == null) return Collections.emptyList();
        List<EventJob> result = new ArrayList<>();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
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
    public DependentJob findDependentJob(String instance, String group, String name) throws JobException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(findDependentJob);
            setValue(ps, instance, group, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new DependentJob(
                        rs.getString(1)
                        , rs.getString(2)
                        ,rs.getString(3)
                        , rs.getString(4)
                );
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
            , EventJob eventJob
            , long nextTime
    ) throws JobException {
        try {
            String data = om.writeValueAsString(eventJob.getData());
            executeUpdate(insert
                    , instance
                    , eventJob.getGroup()
                    , eventJob.getName()
                    , eventJob.getEvent()
                    , eventJob.getTimezone()
                    , eventJob.getDescription()
                    , eventJob.getCron()
                    , eventJob.getAfterGroup()
                    , eventJob.getAfterName()
                    , nextTime
                    , eventJob.getEnabled()
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
            , EventJob eventJob
            , long nextTime
    ) throws JobException {
        try {
            String data = om.writeValueAsString(eventJob.getData());
            executeUpdate(update
                    , eventJob.getEvent()
                    , eventJob.getTimezone()
                    , eventJob.getDescription()
                    , eventJob.getCron()
                    , nextTime
                    , eventJob.getEnabled()
                    , data
                    , instance
                    , eventJob.getGroup()
                    , eventJob.getName()
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

    @Override
    public int release(String instance, String group, String name, long nextTime) {
        return executeUpdate(updateRelease, EventJobStatus.WAITING, instance, group, name, EventJobStatus.EXECUTING, nextTime);
    }

    @Override
    public int compete(
            String instance, String group, String name
            , long nextTime, long startTime
    ) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(updateExecuting);
            setValue(ps, EventJobStatus.EXECUTING, nextTime, startTime, instance, group, name, EventJobStatus.WAITING, nextTime);
            return ps.executeUpdate();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    @Override
    public int finish(String instance, String group, String name, long endTime) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(updateExecuted);
            setValue(ps, EventJobStatus.WAITING, endTime, instance, group, name);
            return ps.executeUpdate();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    public JobStatus getStatus(
            String instance, String group, String name
    ) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(findStatus);
            setValue(ps, instance, group, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new JobStatus(
                        rs.getInt(1)
                        , rs.getLong(2)
                        , rs.getBoolean(3)
                );
            }
            return null;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new JobException(e);
        } finally {
            close(connection);
        }
    }

    private int executeUpdate(String sql, Object... parameters) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            setValue(ps, parameters);
            return ps.executeUpdate();
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
                , rs.getString(index++)
                , rs.getBoolean(index++)
                , om.readValue(rs.getString(index), dataType)
        );
    }
}
