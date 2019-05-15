package bio.terra.configuration;

import bio.terra.StartupInitializer;
import bio.terra.stairway.Stairway;
import bio.terra.upgrade.Migrate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ApplicationConfiguration {
//    private Logger logger = LoggerFactory.getLogger("bio.terra.configuration.ApplicationConfiguration");

    @Bean("stairway")
    public Stairway getStairway(Migrate migrate, ApplicationContext applicationContext) {
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

    @Value("${userEmail}")
    private String userEmail;

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    @Value("${datarepo.dnsname}")
    private String datarepoDnsName;

    @Bean("datarepoDnsName")
    public String datarepoDnsName() {
        return datarepoDnsName;
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
