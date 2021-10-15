package bio.terra.service.dataset.flight.ingest;

public final class IngestMapKeys {

  private IngestMapKeys() {}

  public static final String PREFIX = "ingest-";

  public static final String INGEST_STATISTICS = PREFIX + "ingestStatistics";
  public static final String STAGING_TABLE_NAME = PREFIX + "stagingTableName";
  public static final String NUM_BULK_LOAD_FILE_MODELS = PREFIX + "numBulkLoadFileModels";
  public static final String BULK_LOAD_RESULT = PREFIX + "bulkLoadResult";
  public static final String INGEST_SCRATCH_FILE_PATH = PREFIX + "ingestScratchFilePath";
  public static final String PARQUET_FILE_PATH = PREFIX + "parquetFilePath";
  public static final String COMBINED_FAILED_ROW_COUNT = PREFIX + "combinedFailedRowCount";
  public static final String COMBINED_EXISTING_FILES = PREFIX + "existingFileModels";
}
