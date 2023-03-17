package bio.terra.service.dataset;

import bio.terra.common.PdaoConstant;
import bio.terra.model.DatasetRequestModel;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.stringtemplate.v4.ST;

public final class DatasetUtils {

  // Only allow usage of static methods.
  private DatasetUtils() {}

  private static final String auxTableNamePattern =
      PdaoConstant.PDAO_PREFIX + "<auxId>_<table>_<randomSuffix>";

  private static final String metadataTableNamePattern =
      PdaoConstant.PDAO_PREFIX + "<auxId>_<table>";

  /**
   * Generate a semi-random name for an 'auxiliary' BigQuery table to support a user-defined table.
   *
   * <p>'Auxiliary' tables can store helper information during ingest / at runtime. For example:
   * staging tables, "raw" data tables, soft-delete lists.
   *
   * @param table user-defined table to generate an aux name for
   * @param infixId identifier to include in the aux table name, to help distinguish it in the
   *     BigQuery UI
   */
  public static String generateAuxTableName(DatasetTable table, String infixId) {
    ST nameTemplate = new ST(auxTableNamePattern);
    nameTemplate.add("table", table.getName());
    nameTemplate.add("auxId", infixId);
    nameTemplate.add(
        "randomSuffix", StringUtils.replaceChars(UUID.randomUUID().toString(), '-', '_'));
    return nameTemplate.render();
  }

  public static String generateMetadataTableName(DatasetTable table, String infixId) {
    ST nameTemplate = new ST(metadataTableNamePattern);
    nameTemplate.add("table", table.getName());
    nameTemplate.add("auxId", infixId);
    return nameTemplate.render();
  }

  /**
   * Convert a dataset request into a fully-populated dataset model.
   *
   * <p>Names for "raw" and "soft-delete" tables will be generated and injected into the model after
   * it is parsed from the base request. Since those generated names are semi-random, calling this
   * method twice on the same request will produce different results.
   */
  public static Dataset convertRequestWithGeneratedNames(DatasetRequestModel request) {
    Dataset baseDataset = DatasetJsonConversion.datasetRequestToDataset(request, null);
    fillGeneratedTableNames(baseDataset.getTables());
    return baseDataset;
  }

  public static void fillGeneratedTableNames(List<DatasetTable> tables) {
    tables.forEach(
        t -> {
          t.rawTableName(generateAuxTableName(t, "raw"));
          t.softDeleteTableName(generateAuxTableName(t, "sd"));
          t.rowMetadataTableName(generateMetadataTableName(t, "row_metadata"));
        });
  }
}
