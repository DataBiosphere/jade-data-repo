package runner.config;

public class ApplicationSpecification implements SpecificationInterface {
  public int maxStairwayThreads = 20;
  public int maxBulkFileLoad = 1000000;
  public int maxBulkFileLoadArray = 1000000;
  public int loadConcurrentFiles = 80;
  public int loadConcurrentIngests = 2;
  public int loadDriverWaitSeconds = 1;
  public long loadHistoryCopyChunkSize = 1000;
  public long loadHistoryWaitSeconds = 2;

  ApplicationSpecification() {}

  /** Validate the application specification read in from the JSON file. */
  public void validate() {
    if (maxStairwayThreads <= 0) {
      throw new IllegalArgumentException("Application property maxStairwayThreads must be >= 0");
    } else if (maxBulkFileLoad <= 0) {
      throw new IllegalArgumentException("Application property maxBulkFileLoad must be >= 0");
    } else if (maxBulkFileLoadArray <= 0) {
      throw new IllegalArgumentException("Application property maxBulkFileLoadArray must be >= 0");
    } else if (loadConcurrentFiles <= 0) {
      throw new IllegalArgumentException("Application property loadConcurrentFiles must be >= 0");
    } else if (loadConcurrentIngests <= 0) {
      throw new IllegalArgumentException("Application property loadConcurrentIngests must be >= 0");
    } else if (loadDriverWaitSeconds <= 0) {
      throw new IllegalArgumentException("Application property loadDriverWaitSeconds must be >= 0");
    } else if (loadHistoryCopyChunkSize <= 0) {
      throw new IllegalArgumentException(
          "Application property loadHistoryCopyChunkSize must be >= 0");
    } else if (loadHistoryWaitSeconds <= 0) {
      throw new IllegalArgumentException(
          "Application property loadHistoryWaitSeconds must be >= 0");
    }
  }
}
