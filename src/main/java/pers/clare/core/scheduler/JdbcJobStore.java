package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import pers.clare.core.scheduler.bo.EventJob;
import pers.clare.core.scheduler.bo.Job;
import pers.clare.core.scheduler.constant.EventJobStatus;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Log4j2
public class JdbcJobStore implements JobStore {
    private static final TypeReference<Map<String, Object>> dataType = new TypeReference<Map<String, Object>>() {
    };
    private static final ObjectMapper om = new ObjectMapper();
    private static final Pattern replacePattern = Pattern.compile("`");
    private DataSource dataSource;


    @Override
    public List<Job> findAll(String instance) {
        return null;
    }

    public Boolean exists(String instance, String group, String name) throws SQLException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("select 1 from schedule_job where `instance` = ? and `group` = ? and `name` = ?");
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
            PreparedStatement ps = connection.prepareStatement("select `group`,`name`,timezone,description,concurrent,cron,prev_time,next_time,`data` from schedule_job where `instance` = ? and `group` = ? and `name` = ?");
            ps.setString(1, instance);
            ps.setString(2, group);
            ps.setString(3, name);
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                int index = 0;
                return new EventJob(
                        instance
                        , group
                        , name
                        , rs.getString(index++)
                        , rs.getString(index++)
                        , rs.getBoolean(index++)
                        , rs.getString(index++)
                        , rs.getLong(index++)
                        , rs.getLong(index++)
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
    public void insert(String instance, EventJob eventJob) throws SQLException, JsonProcessingException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("insert into schedule_job(`instance`,`group`,`name`,timezone,description,concurrent,cron,next_time,`data`) values(?,?,?,?,?,?,?,?,?,?,?,?)");
            int index = 1;
            ps.setString(index++, instance);
            ps.setString(index++, eventJob.getGroup());
            ps.setString(index++, eventJob.getName());
            ps.setString(index++, eventJob.getTimezone());
            ps.setString(index++, eventJob.getDescription());
            ps.setBoolean(index++, eventJob.getConcurrent());
            ps.setString(index++, eventJob.getCron());
            ps.setLong(index++, eventJob.getNextTime());
            ps.setString(index++, om.writeValueAsString(eventJob.getData()));
            ps.executeUpdate();
        } catch (Exception e) {
            throw e;
        } finally {
            close(connection);
        }
    }

    @Override
    public void update(String instance, EventJob eventJob) throws SQLException, JsonProcessingException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("update schedule_job set timezone=?,description=?,concurrent=?,cron=?,data=? where `instance` = ? and `group` = ? and `name` = ?");
            int index = 1;
            ps.setString(index++, eventJob.getTimezone());
            ps.setString(index++, eventJob.getDescription());
            ps.setBoolean(index++, eventJob.getConcurrent());
            ps.setString(index++, eventJob.getCron());
            ps.setString(index++, om.writeValueAsString(eventJob.getData()));
            ps.setString(index++, instance);
            ps.setString(index++, eventJob.getGroup());
            ps.setString(index++, eventJob.getName());
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
            PreparedStatement ps = connection.prepareStatement("delete from schedule_job where `instance` = ? and `group` = ? and `name` = ?");
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
        Connection connection = null;
        Boolean autocommit = null;
        Integer transactionIsolation = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement ps;
            //
            if (eventJob.getConcurrent()) {
                ps = connection.prepareStatement("update schedule_job set status=?,prev_time=?,next_time=?,executer=executer+1 where `instance` = ? and `group` = ? and `name` = ?");
            } else {
                // 依賴髒讀來爭奪執行任務權限
                autocommit = connection.getAutoCommit();
                transactionIsolation = connection.getTransactionIsolation();
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                ps = connection.prepareStatement("update schedule_job set status=?,prev_time=?,next_time=?,executer=executer+1 where `instance` = ? and `group` = ? and `name` = ? and status = ?");
            }
            ps.setInt(1, EventJobStatus.EXECUTING);
            ps.setLong(2, eventJob.getPrevTime());
            ps.setLong(3, eventJob.getNextTime());
            ps.setString(4, instance);
            ps.setString(5, eventJob.getGroup());
            ps.setString(6, eventJob.getName());
            if (!eventJob.getConcurrent()) {
                ps.setInt(7, EventJobStatus.WAITING);
            }
            if (ps.executeUpdate() > 0) {
                runnable.run();
                ps = connection.prepareStatement("update schedule_job set status=?,executer=executer-1 where `instance` = ? and `group` = ? and `name` = ? and executer = 1");
                ps.setInt(1, EventJobStatus.WAITING);
                ps.setString(2, instance);
                ps.setString(3, eventJob.getGroup());
                ps.setString(4, eventJob.getName());
            }
            connection.commit();
        } catch (Exception e) {
            rollback(connection);
            throw e;
        } finally {
            revert(connection, autocommit, transactionIsolation);
            close(connection);
        }
    }

    private String getSelectSQL(String instance) {
        StringBuilder sql = new StringBuilder("select `group`,`name`,timezone,description,concurrent,cron,status,prev_time,next_time,`data` from schedule_job where (`instance`,`group`,`name`) in(");

        return sql.toString();
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
