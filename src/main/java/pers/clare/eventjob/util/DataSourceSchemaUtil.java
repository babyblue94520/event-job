package pers.clare.eventjob.util;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.lang.NonNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceSchemaUtil {
    private static final Logger log = LogManager.getLogger();

    public static void init(@NonNull DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            commit(connection, "schema/event-job/" + connection.getMetaData().getDatabaseProductName() + ".sql");
        }
    }

    public static void init(@NonNull DataSource dataSource, String path) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            commit(connection, path);
        }
    }

    private static void commit(Connection connection, String path) throws SQLException {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource(path));
        populator.setContinueOnError(true);
        populator.populate(connection);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

}
