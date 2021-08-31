package bio.terra.app.configuration;

import bio.terra.app.utils.startup.StartupInitializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
  /** Maximum number of file loads allowed in the input array in a bulk file load */
  private int maxBulkFileLoadArray;
  /** Maximum number of lines in ingest request */
  private long maxDatasetIngest;
  /** Number of file loads to run concurrently in a bulk file load */
  private int loadConcurrentFiles;
  /**
   * Number of file loads to run concurrently. NOTE: the maximum number of threads used for load is
   * one for the driver flight and N for the number of concurrent files: {@code
   * loadConcurrentIngests * (loadConcurrentFiles + 1)} That result should be less than
   * maxStairwayThreads, lest loads take over all Stairway threads!
   */
  private int loadConcurrentIngests;

  /**
   * Number of seconds for the bulk file load driver thread to wait to check on completed load
   * flights
   */
  private int loadDriverWaitSeconds;

  /** Number of seconds to wait between loads of data into laod_history table */
  private int loadHistoryWaitSeconds;

  /** Number of files for the bulk file load to load file metadata into staging table */
  private int loadHistoryCopyChunkSize;

  /**
   * Number of badly formed lines in a bulk load input file to return in the error details of the
   * error model
   */
  private int maxBadLoadFileLineErrorsReported;

  /** Number of rows to collect into a batch for loading into the load_file table */
  private int loadFilePopulateBatchSize;

  /**
   * Name of the Kubernetes pod we are running in. If we are not in a pod, this defaults to a
   * constant string in application properties.
   */
  private String podName;

  /**
   * Used to denote that we are running in the Kubernetes environment. This should NOT be changed in
   * application.properties. It should only be reset by the Kubernetes deployment.
   */
  private boolean inKubernetes;

  /**
   * Pod shutdown timeout. When constructed using our helm charts, the shutdown time is set both in
   * the Kubernetes configuration and as an environment variable that controls this value.
   */
  private int shutdownTimeoutSeconds;

  /** Size of batches to operate on when creating snapshot file system directory entries */
  private int firestoreSnapshotBatchSize;

  /** Size of cache of directories maintain when building the snapshot file system */
  private int firestoreSnapshotCacheSize;

  /** Size of batches to operate on when validating file system directory entry ids */
  private int firestoreValidateBatchSize;

  /** Sizes of batches of query results from firestore */
  private int firestoreQueryBatchSize;

  /** Maximum number of DRS lookup requests allowed */
  private int maxDrsLookups;

  /** Size of users in auth cache */
  private int authCacheSize;

  /** Time in seconds of auth cache timeout */
  private int authCacheTimeoutSeconds;

  /**
   * Certain operations can be spread to run asynchronously to gain a performance boost. Instead of
   * having each such task create its own threadpool, this property is used to create a globally
   * accessible pool that should be used by all such operations.
   *
   * <p>Note: this is different than than the flight threadpool.
   */
  private int numPerformanceThreads;

  /**
   * The maximum size of the queue before submitting threads get rejected. Note: in all, you can
   * submit: maxPerformanceThreadQueueSize + numPerformanceThreads before you get an exception
   */
  private int maxPerformanceThreadQueueSize;

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

  public int getLoadHistoryWaitSeconds() {
    return loadHistoryWaitSeconds;
  }

  public void setLoadHistoryWaitSeconds(int loadQueryHistorySeconds) {
    this.loadHistoryWaitSeconds = loadQueryHistorySeconds;
  }

  public int getLoadHistoryCopyChunkSize() {
    return loadHistoryCopyChunkSize;
  }

  public void setLoadHistoryCopyChunkSize(int loadHistoryCopyChunkSize) {
    this.loadHistoryCopyChunkSize = loadHistoryCopyChunkSize;
  }

  public int getMaxBadLoadFileLineErrorsReported() {
    return maxBadLoadFileLineErrorsReported;
  }

  public void setMaxBadLoadFileLineErrorsReported(int maxBadLoadFileLineErrorsReported) {
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
  }

  public int getLoadFilePopulateBatchSize() {
    return loadFilePopulateBatchSize;
  }

  public void setLoadFilePopulateBatchSize(int loadFilePopulateBatchSize) {
    this.loadFilePopulateBatchSize = loadFilePopulateBatchSize;
  }

  public int getShutdownTimeoutSeconds() {
    return shutdownTimeoutSeconds;
  }

  public void setShutdownTimeoutSeconds(int shutdownTimeoutSeconds) {
    this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
  }

  public String getPodName() {
    return podName;
  }

  public void setPodName(String podName) {
    this.podName = podName;
  }

  public boolean isInKubernetes() {
    return inKubernetes;
  }

  public void setInKubernetes(boolean inKubernetes) {
    this.inKubernetes = inKubernetes;
  }

  public int getFirestoreSnapshotBatchSize() {
    return firestoreSnapshotBatchSize;
  }

  public void setFirestoreSnapshotBatchSize(int firestoreSnapshotBatchSize) {
    this.firestoreSnapshotBatchSize = firestoreSnapshotBatchSize;
  }

  public int getFirestoreSnapshotCacheSize() {
    return firestoreSnapshotCacheSize;
  }

  public void setFirestoreSnapshotCacheSize(int firestoreSnapshotCacheSize) {
    this.firestoreSnapshotCacheSize = firestoreSnapshotCacheSize;
  }

  public int getFirestoreValidateBatchSize() {
    return firestoreValidateBatchSize;
  }

  public void setFirestoreValidateBatchSize(int firestoreValidateBatchSize) {
    this.firestoreValidateBatchSize = firestoreValidateBatchSize;
  }

  public int getFirestoreQueryBatchSize() {
    return firestoreQueryBatchSize;
  }

  public void setFirestoreQueryBatchSize(int firestoreQueryBatchSize) {
    this.firestoreQueryBatchSize = firestoreQueryBatchSize;
  }

  /*
   * WARNING: if making any changes to these methods make sure to notify the #dsp-batch channel! Describe the change
   * and any consequences downstream to DRS clients.
   */
  public int getMaxDrsLookups() {
    return maxDrsLookups;
  }

  public void setMaxDrsLookups(int maxDrsLookups) {
    this.maxDrsLookups = maxDrsLookups;
  }

  public int getAuthCacheSize() {
    return authCacheSize;
  }

  public void setAuthCacheSize(int authCacheSize) {
    this.authCacheSize = authCacheSize;
  }

  public int getAuthCacheTimeoutSeconds() {
    return authCacheTimeoutSeconds;
  }

  public void setAuthCacheTimeoutSeconds(int authCacheTimeoutSeconds) {
    this.authCacheTimeoutSeconds = authCacheTimeoutSeconds;
  }

  public int getNumPerformanceThreads() {
    return numPerformanceThreads;
  }

  public void setNumPerformanceThreads(int numPerformanceThreads) {
    this.numPerformanceThreads = numPerformanceThreads;
  }

  public int getMaxPerformanceThreadQueueSize() {
    return maxPerformanceThreadQueueSize;
  }

  public void setMaxPerformanceThreadQueueSize(int maxPerformanceThreadQueueSize) {
    this.maxPerformanceThreadQueueSize = maxPerformanceThreadQueueSize;
  }

  @Bean("jdbcTemplate")
  public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(
      DataRepoJdbcConfiguration jdbcConfiguration) {
    return new NamedParameterJdbcTemplate(jdbcConfiguration.getDataSource());
  }

  @Bean("objectMapper")
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule());
  }

  @Bean("daoObjectMapper")
  public ObjectMapper daoObjectMapper() {
    return new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_VALUES, true);
  }

  @Bean("performanceThreadpool")
  public ExecutorService performanceThreadpool() {
    return new ThreadPoolExecutor(
        getNumPerformanceThreads(),
        getNumPerformanceThreads(),
        0,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(getMaxPerformanceThreadQueueSize()));
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
