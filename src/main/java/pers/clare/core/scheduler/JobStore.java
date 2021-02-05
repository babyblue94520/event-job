package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.sql.SQLException;
import java.util.List;

public interface JobStore {

    Boolean exists(String instance, String group, String name) throws SQLException;

    EventJob find(String instance, String group, String name) throws SQLException, JsonProcessingException;

    void insert(String instance, Job job, long nextTime) throws SQLException, JsonProcessingException;

    void update(String instance, Job job, long nextTime) throws SQLException, JsonProcessingException;

    void delete(String instance, String group, String name) throws SQLException;

    void executeLock(
            String instance
            , EventJob eventJob
            , Runnable runnable
    ) throws SQLException;
}
