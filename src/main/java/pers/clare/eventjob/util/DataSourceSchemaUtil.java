package pers.clare.eventjob.util;


import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.lang.NonNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Log4j2
@UtilityClass
public class DataSourceSchemaUtil {

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
