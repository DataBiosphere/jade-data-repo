package bio.terra.service.dataset.flight.ingest;

public final class IngestMapKeys {
  private IngestMapKeys() {}

  public static final String INGEST_STATISTICS = "ingestStatistics";
  public static final String STAGING_TABLE_NAME = "stagingTableName";
  public static final String BULK_LOAD_FILE_MODELS = "bulkLoadFileModels";
  public static final String BULK_LOAD_RESULT = "bulkLoadResult";
  public static final String BULK_LOAD_JSON_LINES = "bulkLoadJsonLines";
  public static final String TABLE_SCHEMA_FILE_COLUMNS = "tableSchemaFileColumns";
  public static final String LINES_WITH_FILE_IDS = "linesWithFileIds";
  public static final String INGEST_SCRATCH_FILE_PATH = "ingestScratchFilePath";
}
