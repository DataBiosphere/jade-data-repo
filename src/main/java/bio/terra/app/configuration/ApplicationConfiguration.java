package bio.terra.app.configuration;

import bio.terra.app.utils.startup.StartupInitializer;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "datarepo")
public class ApplicationConfiguration {

    private String userEmail;
    private String dnsName;
    private String resourceId;
    private String userId;

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getDnsName() {
        return dnsName;
    }

    public void setDnsName(String dnsName) {
        this.dnsName = dnsName;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Bean("stairway")
    public Stairway getStairway(ApplicationContext applicationContext) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        return new Stairway(executorService, applicationContext);
    }

    @Bean("jdbcTemplate")
    public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(DataRepoJdbcConfiguration jdbcConfiguration) {
        return new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
    }

    @Bean("objectMapper")
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new ParameterNamesModule())
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());
    }

    // This is a "magic bean": It supplies a method that Spring calls after the application is setup,
    // but before the port is opened for business. That lets us do database migration and stairway
    // initialization on a system that is otherwise fully configured. The rule of thumb is that all
    // bean initialization should avoid database access. If there is additional database work to be
    // done, it should happen inside this method.
    @Bean
    public SmartInitializingSingleton postSetupInitialization(ApplicationContext applicationContext) {
        return () -> {
            StartupInitializer.initialize(applicationContext);
        };
    }

}
