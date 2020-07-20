package runner.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationSpecification implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpecification.class);

  public int maxStairwayThreads = 20;
  public int maxBulkFileLoad = 1000000;
  public int loadConcurrentFiles = 4;
  public int loadConcurrentIngests = 2;
  public long loadHistoryCopyChunkSize = 1000;
  public long loadHistoryWaitSeconds = 2;

  ApplicationSpecification() {}

  /** Validate the application specification read in from the JSON file. */
  public void validate() {
    if (maxStairwayThreads <= 0) {
      throw new IllegalArgumentException("Application property maxStairwayThreads must be >= 0");
    } else if (maxBulkFileLoad <= 0) {
      throw new IllegalArgumentException("Application property maxBulkFileLoad must be >= 0");
    } else if (loadConcurrentFiles <= 0) {
      throw new IllegalArgumentException("Application property loadConcurrentFiles must be >= 0");
    } else if (loadConcurrentIngests <= 0) {
      throw new IllegalArgumentException("Application property loadConcurrentIngests must be >= 0");
    } else if (loadHistoryCopyChunkSize <= 0) {
      throw new IllegalArgumentException(
          "Application property loadHistoryCopyChunkSize must be >= 0");
    } else if (loadHistoryWaitSeconds <= 0) {
      throw new IllegalArgumentException(
          "Application property loadHistoryWaitSeconds must be >= 0");
    }
  }

  public void display() {
    LOG.info("Application: ");
    LOG.info("  maxStairwayThreads: {}", maxStairwayThreads);
    LOG.info("  maxBulkFileLoad: {}", maxBulkFileLoad);
    LOG.info("  loadConcurrentFiles: {}", loadConcurrentFiles);
    LOG.info("  loadConcurrentIngests: {}", loadConcurrentIngests);
    LOG.info("  loadHistoryCopyChunkSize: {}", loadHistoryCopyChunkSize);
    LOG.info("  loadHistoryWaitSeconds: {}", loadHistoryWaitSeconds);
  }
}
