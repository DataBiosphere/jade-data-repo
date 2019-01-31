package bio.terra.configuration;

import bio.terra.stairway.Stairway;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ApplicationConfiguration {

    @Bean("stairway")
    public Stairway getStairway(StairwayJdbcConfiguration jdbcConfiguration, ApplicationContext applicationContext) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        DataSource dataSource = jdbcConfiguration.getDataSource();
        return new Stairway(executorService, dataSource, false, applicationContext);
    }

    @Bean("jdbcTemplate")
    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(DataRepoJdbcConfiguration jdbcConfiguration) {
        return new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

}
