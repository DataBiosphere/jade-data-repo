package bio.terra.app.configuration;

import bio.terra.app.utils.startup.StartupInitializer;
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

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "datarepo")
public class ApplicationConfiguration {

    private String userEmail;
    private String dnsName;
    private String resourceId;
    private String userId;
    /**
     * Size of the Stairway thread pool. The pool is consumed by requests and by file load threads.
     */
    private int maxStairwayThreads;
    /**
     * Maximum number of file loads allowed in the input array in a bulk file load
     */
    private int maxBulkFileLoadArray;
    /**
     * Maximum number of file loads allowed in the input file for a bulk file load
     */
    private int maxBulkFileLoad;
    /**
     * Number of file loads to run concurrently in a bulk file load
     */
    private int loadConcurrentFiles;
    /**
     * Number of file loads to run concurrently.
     * NOTE: the maximum number of threads used for load is one for the driver flight and N for
     * the number of concurrent files:
     *   {@code loadConcurrentIngests * (loadConcurrentFiles + 1)}
     * That result should be less than maxStairwayThreads, lest loads take over all Stairway
     * threads!
     */
    private int loadConcurrentIngests;

    /**
     * Number of seconds for the bulk file load driver thread to wait to check on completed load flights
     */
    private int loadDriverWaitSeconds;

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

    public int getMaxStairwayThreads() {
        return maxStairwayThreads;
    }

    public void setMaxStairwayThreads(int maxStairwayThreads) {
        this.maxStairwayThreads = maxStairwayThreads;
    }

    public int getMaxBulkFileLoadArray() {
        return maxBulkFileLoadArray;
    }

    public void setMaxBulkFileLoadArray(int maxBulkFileLoadArray) {
        this.maxBulkFileLoadArray = maxBulkFileLoadArray;
    }

    public int getMaxBulkFileLoad() {
        return maxBulkFileLoad;
    }

    public void setMaxBulkFileLoad(int maxBulkFileLoad) {
        this.maxBulkFileLoad = maxBulkFileLoad;
    }

    public int getLoadConcurrentFiles() {
        return loadConcurrentFiles;
    }

    public void setLoadConcurrentFiles(int loadConcurrentFiles) {
        this.loadConcurrentFiles = loadConcurrentFiles;
    }

    public int getLoadConcurrentIngests() {
        return loadConcurrentIngests;
    }

    public void setLoadConcurrentIngests(int loadConcurrentIngests) {
        this.loadConcurrentIngests = loadConcurrentIngests;
    }

    public int getLoadDriverWaitSeconds() {
        return loadDriverWaitSeconds;
    }

    public void setLoadDriverWaitSeconds(int loadDriverWaitSeconds) {
        this.loadDriverWaitSeconds = loadDriverWaitSeconds;
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
