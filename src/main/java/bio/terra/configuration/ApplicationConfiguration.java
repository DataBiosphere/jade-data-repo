package bio.terra.configuration;

import bio.terra.stairway.Stairway;
import bio.terra.upgrade.Migrate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ApplicationConfiguration {
    private Logger logger = LoggerFactory.getLogger("bio.terra.configuration.ApplicationConfiguration");


    @Value("${db.stairway.forceClean}")
    private String stairwayForceClean;

    @Bean("stairway")
    public Stairway getStairway(Migrate migrate, ApplicationContext applicationContext) {
        StairwayJdbcConfiguration jdbcConfiguration = migrate.getStairwayJdbcConfiguration();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        DataSource dataSource = jdbcConfiguration.getDataSource();
        boolean forceClean = Boolean.parseBoolean(stairwayForceClean);
        logger.debug("ApplicationConfiguration stairwayForceClean is '" + stairwayForceClean +
                "'; forceClean is " + forceClean);
        return new Stairway(executorService, dataSource, forceClean, applicationContext);
    }

    @Bean("jdbcTemplate")
    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(DataRepoJdbcConfiguration jdbcConfiguration) {
        return new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    @Bean("modelDateFormat")
    public FastDateFormat modelDateFormat() {
        return FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    @Bean("objectMapper")
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new ParameterNamesModule())
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());
    }

    @Value("${userEmail}")
    private String userEmail;

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

}
