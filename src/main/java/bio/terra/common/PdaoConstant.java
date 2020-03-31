package bio.terra.common;

public final class PdaoConstant {

    // checkstyle will complain if you remove this.
    private PdaoConstant() {}

    public static final String PDAO_PREFIX = "datarepo_";
    public static final String PDAO_ROW_ID_COLUMN = PDAO_PREFIX + "row_id";
    public static final String PDAO_ROW_ID_TABLE = PDAO_PREFIX + "row_ids";
    public static final String PDAO_TABLE_ID_COLUMN = PDAO_PREFIX + "table_id";
    public static final String PDAO_INGEST_DATE_COLUMN_ALIAS = PDAO_PREFIX + "ingest_date";
    public static final String PDAO_EXTERNAL_TABLE_PREFIX = PDAO_PREFIX + "ext_";
}
