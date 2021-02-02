package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import pers.clare.core.scheduler.bo.EventJob;
import pers.clare.core.scheduler.bo.Job;

import java.sql.SQLException;
import java.util.List;

public interface JobStore {

    List<Job> findAll(String instance);

    Boolean exists(String instance, String group, String name) throws SQLException;

    EventJob find(String instance, String group, String name) throws SQLException, JsonProcessingException;

    void insert(String instance, EventJob job) throws SQLException, JsonProcessingException;

    void update(String instance, EventJob job) throws SQLException, JsonProcessingException;

    void delete(String instance, String group, String name) throws SQLException;
}
