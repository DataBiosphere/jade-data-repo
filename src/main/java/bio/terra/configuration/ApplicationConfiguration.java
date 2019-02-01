package bio.terra.configuration;

import bio.terra.stairway.Stairway;
import bio.terra.upgrade.Migrate;
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
    public Stairway getStairway(Migrate migrate, ApplicationContext applicationContext) {
        StairwayJdbcConfiguration jdbcConfiguration = migrate.getStairwayJdbcConfiguration();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        DataSource dataSource = jdbcConfiguration.getDataSource();
        // TODO: this magic "true" here is forcing clean starts from stairway. I find it useful now at this stage,
        // but think we should drive it from configuration based on environment (always do it locally and in dev, not
        // in staging or prod).
        return new Stairway(executorService, dataSource, true, applicationContext);
    }

    @Bean("jdbcTemplate")
    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(DataRepoJdbcConfiguration jdbcConfiguration) {
        return new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

}
