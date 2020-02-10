package bio.terra.common;

public final class PdaoConstant {

    private PdaoConstant() {
    }

    public static final String PDAO_PREFIX = "datarepo_";
    public static final String STAGING_TABLE_PREFIX = "staging_table_";
    public static final String TARGET_TABLE_PREFIX = "target_table_";
    public static final String PDAO_ROW_ID_COLUMN = PDAO_PREFIX + "row_id";
    public static final String PDAO_ROW_ID_TABLE = PDAO_PREFIX + "row_ids";
    public static final String PDAO_TABLE_ID_COLUMN = PDAO_PREFIX + "table_id";
    public static final String STAGING_TABLE_ROW_ID_COLUMN = PDAO_PREFIX + STAGING_TABLE_PREFIX + "row_id";
    public static final String TARGET_TABLE_ROW_ID_COLUMN = PDAO_PREFIX + TARGET_TABLE_PREFIX + "row_id";

}
