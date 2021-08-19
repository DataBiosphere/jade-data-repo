package bio.terra.service.dataset.flight.ingest;

public final class IngestMapKeys {
  private IngestMapKeys() {}

  public static final String PREFIX = "ingest-";

  public static final String INGEST_STATISTICS = PREFIX + "ingestStatistics";
  public static final String STAGING_TABLE_NAME = PREFIX + "stagingTableName";
  public static final String BULK_LOAD_FILE_MODELS = PREFIX + "bulkLoadFileModels";
  public static final String BULK_LOAD_RESULT = PREFIX + "bulkLoadResult";
  public static final String BULK_LOAD_JSON_LINES = PREFIX + "bulkLoadJsonLines";
  public static final String TABLE_SCHEMA_FILE_COLUMNS = PREFIX + "tableSchemaFileColumns";
  public static final String LINES_WITH_FILE_IDS = PREFIX + "linesWithFileIds";
  public static final String INGEST_SCRATCH_FILE_PATH = PREFIX + "ingestScratchFilePath";
  public static final String PARQUET_FILE_PATH = PREFIX + "parquetFilePath";
}
