package bio.terra.common;

public final class PdaoConstant {

  // checkstyle will complain if you remove this.
  private PdaoConstant() {}

  public static final String PDAO_PREFIX = "datarepo_";
  public static final String PDAO_ROW_ID_COLUMN = PDAO_PREFIX + "row_id";
  public static final String PDAO_ROW_ID_TABLE = PDAO_PREFIX + "row_ids";
  public static final String PDAO_ROW_ID_PARQUET_NAME = PDAO_PREFIX + "row_ids";
  public static final String PDAO_TOTAL_ROW_COUNT_COLUMN_NAME = "total_row_count";
  public static final String PDAO_FILTERED_ROW_COUNT_COLUMN_NAME = "filtered_row_count";
  public static final String PDAO_TEMP_TABLE = PDAO_PREFIX + "temp";
  public static final String PDAO_TABLE_ID_COLUMN = PDAO_PREFIX + "table_id";
  public static final String PDAO_INGEST_DATE_COLUMN_ALIAS = PDAO_PREFIX + "ingest_date";
  public static final String PDAO_EXTERNAL_TABLE_PREFIX = PDAO_PREFIX + "etranst_";
  public static final String PDAO_LOAD_HISTORY_TABLE = PDAO_PREFIX + "load_history";
  public static final String PDAO_TRANSACTIONS_TABLE = PDAO_PREFIX + "transactions";
  public static final String PDAO_TRANSACTION_ID_COLUMN = PDAO_PREFIX + "transaction_id";
  public static final String PDAO_TRANSACTION_STATUS_COLUMN = PDAO_PREFIX + "transaction_status";
  public static final String PDAO_TRANSACTION_DESCRIPTION_COLUMN = PDAO_PREFIX + "transaction_name";
  public static final String PDAO_TRANSACTION_LOCK_COLUMN = PDAO_PREFIX + "lock";
  public static final String PDAO_TRANSACTION_CREATED_AT_COLUMN =
      PDAO_PREFIX + "transaction_created_at";
  public static final String PDAO_TRANSACTION_CREATED_BY_COLUMN =
      PDAO_PREFIX + "transaction_created_by";
  public static final String PDAO_TRANSACTION_TERMINATED_AT_COLUMN =
      PDAO_PREFIX + "transaction_terminated_at";
  public static final String PDAO_TRANSACTION_TERMINATED_BY_COLUMN =
      PDAO_PREFIX + "transaction_terminated_by";
  public static final String PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX =
      PDAO_LOAD_HISTORY_TABLE + "_staging_";
  public static final String PDAO_FILE_ID_STAGING_TABLE = PDAO_PREFIX + "file_id_staging";
  public static final String PDAO_FILE_ID_STAGING_ORIG_ID = "orig_id";
  public static final String PDAO_FILE_ID_STAGING_NEW_ID = "new_id";
  public static final String PDAO_INGESTED_BY_COLUMN = "ingested_by";
  public static final String PDAO_INGEST_TIME_COLUMN = "ingest_time";
  public static final String PDAO_LOAD_TAG_COLUMN = "load_tag";
  public static final String PDAO_FLIGHT_ID_COLUMN = "flight_id";
  public static final String PDAO_DELETED_AT_COLUMN = "deleted_at";
  public static final String PDAO_DELETED_BY_COLUMN = "deleted_by";
  public static final String PDAO_FIRESTORE_DUMP_FILE_ID_KEY = "file_id";
  public static final String PDAO_FIRESTORE_DUMP_GSPATH_KEY = "gs_path";
  public static final String PDAO_GS_MAPPING_TABLE = PDAO_PREFIX + "gs_path_mapping";
  public static final String PDAO_COUNT_ALIAS = "count";
  public static final String PDAO_COUNT_COLUMN_NAME = "count_column";
  public static final String PDAO_MAX_VALUE_COLUMN_NAME = "max";
  public static final String PDAO_MIN_VALUE_COLUMN_NAME = "min";
}
