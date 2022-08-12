package pers.clare.h2;

import org.h2.tools.Server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import pers.clare.eventjob.EnableEventJob;

import java.sql.SQLException;

@EnableEventJob
@SpringBootApplication
public class H2Application {
    public static void main(String[] args) {
        SpringApplication.run(H2Application.class, "--spring.profiles.active=h2server");
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public Server inMemoryH2DatabaseServer() throws SQLException {
        return Server.createTcpServer(
                "-tcp", "-tcpAllowOthers", "-tcpPort", "9090");
    }
}
