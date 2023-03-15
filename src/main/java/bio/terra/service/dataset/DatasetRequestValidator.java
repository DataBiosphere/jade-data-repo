package bio.terra.service.dataset;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.PdaoConstant;
import bio.terra.common.ValidationUtils;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.DatePartitionOptionsModel;
import bio.terra.model.IntPartitionOptionsModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

/**
 * This validator runs along with the constraint validation that comes from the Models generated by
 * swagger-codegen. The constraints will be able to handle things like nulls, but not things like
 * uniqueness or other structural validations.
 *
 * <p>There are a lot of null checks here because this will still be called even if a constraint
 * validation failed. Spring will not shortcut out early if a validation fails, so this Validator
 * will still get nulls and should only validate if the field is present.
 */
@Component
public class DatasetRequestValidator implements Validator {

  private static String PRIMARY_KEY = "PrimaryKey";
  private static String FOREIGN_KEY = "ForeignKey";

  @Override
  public boolean supports(Class<?> clazz) {
    return true;
  }

  public static class SchemaValidationContext {

    private HashMap<String, HashSet<String>> tableColumnMap;
    private HashMap<String, HashSet<String>> tableArrayColumns;
    private HashSet<String> relationshipNameSet;

    SchemaValidationContext() {
      tableColumnMap = new HashMap<>();
      tableArrayColumns = new HashMap<>();
      relationshipNameSet = new HashSet<>();
    }

    void addTable(String tableName, List<ColumnModel> columns) {
      HashSet<String> colNames = new HashSet<>();
      HashSet<String> arrayCols = new HashSet<>();

      for (ColumnModel col : columns) {
        colNames.add(col.getName());
        if (col.isArrayOf()) {
          arrayCols.add(col.getName());
        }
      }

      tableColumnMap.put(tableName, colNames);
      tableArrayColumns.put(tableName, arrayCols);
    }

    void addRelationship(String relationshipName) {
      relationshipNameSet.add(relationshipName);
    }

    boolean isValidTable(String tableName) {
      return tableColumnMap.containsKey(tableName);
    }

    boolean isValidTableColumn(String tableName, String columnName) {
      return isValidTable(tableName) && tableColumnMap.get(tableName).contains(columnName);
    }

    boolean isArrayColumn(String tableName, String columnName) {
      return isValidTableColumn(tableName, columnName)
          && tableArrayColumns.get(tableName).contains(columnName);
    }

    boolean isValidRelationship(String relationshipName) {
      return relationshipNameSet.contains(relationshipName);
    }
  }

  private void validateDatasetName(String datasetName, Errors errors) {
    // NOTE: We used to manually check the name against a pattern here, but the latest
    // versions of Swagger codegen now auto-generate an equivalent check.
    if (datasetName == null) {
      errors.rejectValue("name", "DatasetNameMissing");
    }
  }

  private void validateDatePartitionOptions(
      DatePartitionOptionsModel options, List<ColumnModel> columns, Errors errors) {
    String targetColumn = options.getColumn();

    if (targetColumn == null) {
      errors.rejectValue("schema", "MissingDatePartitionColumnName");
    } else if (!targetColumn.equals(PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS)) {
      Optional<ColumnModel> matchingColumn =
          columns.stream().filter(c -> targetColumn.equals(c.getName())).findFirst();

      if (matchingColumn.isPresent()) {
        TableDataType colType = matchingColumn.get().getDatatype();

        if (colType != TableDataType.DATE && colType != TableDataType.TIMESTAMP) {
          errors.rejectValue(
              "schema",
              "InvalidDatePartitionColumnType",
              "partitionColumn in datePartitionOptions must refer to a DATE or TIMESTAMP column");
        }
      } else {
        errors.rejectValue(
            "schema", "InvalidDatePartitionColumnName", "No such column: " + targetColumn);
      }
    }
  }

  private void validateIntPartitionOptions(
      IntPartitionOptionsModel options, List<ColumnModel> columns, Errors errors) {
    String targetColumn = options.getColumn();

    if (targetColumn == null) {
      errors.rejectValue("schema", "MissingIntPartitionColumnName");
    } else {
      Optional<ColumnModel> matchingColumn =
          columns.stream().filter(c -> targetColumn.equals(c.getName())).findFirst();

      if (matchingColumn.isPresent()) {
        TableDataType colType = matchingColumn.get().getDatatype();

        if (colType != TableDataType.INTEGER && colType != TableDataType.INT64) {
          errors.rejectValue(
              "schema",
              "InvalidIntPartitionColumnType",
              "partitionColumn in intPartitionOptions must refer to an INTEGER or INT64 column");
        }
      } else {
        errors.rejectValue(
            "schema", "InvalidIntPartitionColumnName", "No such column: " + targetColumn);
      }
    }

    Long min = options.getMin();
    Long max = options.getMax();
    Long interval = options.getInterval();

    if (min == null || max == null || interval == null) {
      errors.rejectValue(
          "schema",
          "MissingIntPartitionOptions",
          "intPartitionOptions must specify min, max, and interval");
    } else {
      if (max <= min) {
        errors.rejectValue(
            "schema",
            "InvalidIntPartitionRange",
            "Max partition value must be larger than min partition value");
      }
      if (interval <= 0) {
        errors.rejectValue(
            "schema", "InvalidIntPartitionInterval", "Partition interval must be >= 1");
      }
      if (max > min && interval > 0 && (max - min) / interval > 4000L) {
        errors.rejectValue(
            "schema",
            "TooManyIntPartitions",
            "Cannot configure more than 4K partitions through min, max, and interval");
      }
    }
  }

  public void validateTable(TableModel table, Errors errors, SchemaValidationContext context) {
    String tableName = table.getName();
    List<ColumnModel> columns = table.getColumns();
    List<String> primaryKeyList = table.getPrimaryKey();
    List<String> columnNames = new ArrayList<>();
    if (columns.isEmpty()) {
      errors.rejectValue(
          "schema", "IncompleteSchemaDefinition", "Each table must contain at least one column");
    } else {
      columns.stream().map(ColumnModel::getName).forEach(columnNames::add);
    }

    if (tableName != null) {
      validateDataTypes(columns, errors);

      if (ValidationUtils.hasDuplicates(columnNames)) {
        List<String> duplicates = ValidationUtils.findDuplicates(columnNames);
        errors.rejectValue(
            "schema",
            "DuplicateColumnNames",
            String.format("Duplicate columns: %s", String.join(", ", duplicates)));
      }
      if (primaryKeyList != null) {
        if (!columnNames.containsAll(primaryKeyList)) {
          List<String> missingKeys = new ArrayList<>(primaryKeyList);
          missingKeys.removeAll(columnNames);
          errors.rejectValue(
              "schema",
              "MissingPrimaryKeyColumn",
              String.format("Expected column(s): %s", String.join(", ", missingKeys)));
        }
      }
      for (ColumnModel columnModel : table.getColumns()) {
        if (primaryKeyList != null && primaryKeyList.contains(columnModel.getName())) {
          validateColumnType(errors, columnModel, PRIMARY_KEY);
        }
        validateColumnMode(errors, columnModel);
      }

      context.addTable(tableName, columns);
    }

    TableModel.PartitionModeEnum mode = table.getPartitionMode();
    DatePartitionOptionsModel dateOptions = table.getDatePartitionOptions();
    IntPartitionOptionsModel intOptions = table.getIntPartitionOptions();

    if (mode == TableModel.PartitionModeEnum.DATE) {
      if (dateOptions == null) {
        errors.rejectValue(
            "schema",
            "MissingDatePartitionOptions",
            "datePartitionOptions must be specified when using 'date' partitionMode");
      } else {
        validateDatePartitionOptions(dateOptions, columns, errors);
      }
    } else if (dateOptions != null) {
      errors.rejectValue(
          "schema",
          "InvalidDatePartitionOptions",
          "datePartitionOptions can only be specified when using 'date' partitionMode");
    }

    if (mode == TableModel.PartitionModeEnum.INT) {
      if (intOptions == null) {
        errors.rejectValue(
            "schema",
            "MissingIntPartitionOptions",
            "intPartitionOptions must be specified when using 'int' partitionMode");
      } else {
        validateIntPartitionOptions(intOptions, columns, errors);
      }
    } else if (intOptions != null) {
      errors.rejectValue(
          "schema",
          "InvalidIntPartitionOptions",
          "intPartitionOptions can only be specified when using 'int' partitionMode");
    }
  }

  // Primary Keys and Foreign Keys cannot be filerefs or dirrefs and Primary keys cannot be arrays
  private void validateColumnType(Errors errors, ColumnModel columnModel, String keyType) {
    if (keyType.equals(PRIMARY_KEY) && columnModel.isArrayOf()) {
      rejectKey(errors, keyType, columnModel.getName(), "array");
    }

    Set<TableDataType> invalidTypes = Set.of(TableDataType.DIRREF, TableDataType.FILEREF);
    if (columnModel.getDatatype() != null && invalidTypes.contains(columnModel.getDatatype())) {
      rejectKey(errors, keyType, columnModel.getName(), columnModel.getDatatype().toString());
    }
    if (PRIMARY_KEY.equals(keyType) && Boolean.FALSE.equals(columnModel.isRequired())) {
      errors.rejectValue(
          "schema",
          "OptionalPrimaryKeyColumn",
          String.format("A %s column cannot be marked as not required", PRIMARY_KEY));
    }
  }

  private void validateColumnMode(Errors errors, ColumnModel columnModel) {
    // Explicitly check if isRequired is true to avoid a null pointer exception.
    // isArrayOf has a default value set in the open-api spec so it does not require
    // the same handling.
    if (Boolean.TRUE.equals(columnModel.isRequired()) && columnModel.isArrayOf()) {
      errors.rejectValue(
          "schema",
          "InvalidColumnMode",
          String.format("Array column %s cannot be marked as required", columnModel.getName()));
    }
  }

  private void rejectKey(Errors errors, String keyType, String columnName, String type) {
    errors.rejectValue(
        "schema",
        String.format("Invalid%s", keyType),
        String.format("%s %s cannot be a column with %s type", keyType, columnName, type));
  }

  private void validateDataTypes(List<ColumnModel> columns, Errors errors) {
    List<ColumnModel> invalidColumns = new ArrayList<>();
    for (ColumnModel column : columns) {
      // spring defaults user input not belonging to the TableDataType enum to null
      if (column.getDatatype() == null) {
        invalidColumns.add(column);
      }
    }
    if (!invalidColumns.isEmpty()) {
      errors.rejectValue(
          "schema",
          "InvalidDatatype",
          "invalid datatype in table column(s): "
              + invalidColumns.stream().map(ColumnModel::getName).collect(Collectors.joining(", "))
              + ", DataTypes must be lowercase, valid DataTypes are "
              + Arrays.toString(TableDataType.values()));
    }
  }

  private void validateRelationship(
      RelationshipModel relationship,
      List<TableModel> tables,
      Errors errors,
      SchemaValidationContext context,
      CloudPlatformWrapper cloudPlatformWrapper) {
    List<Map<String, String>> validationErrors =
        ValidationUtils.getRelationshipValidationErrors(relationship, tables, cloudPlatformWrapper);
    validationErrors.forEach(e -> rejectValues(errors, e));

    String relationshipName = relationship.getName();
    if (relationshipName != null) {
      context.addRelationship(relationshipName);
    }
  }

  private void rejectValues(Errors errors, Map<String, String> errorMap) {
    for (var entry : errorMap.entrySet()) {
      var errorCode = entry.getKey();
      var errorMessage = entry.getValue();
      errors.rejectValue("schema", errorCode, errorMessage);
    }
  }

  private void validateAssetTable(
      AssetTableModel assetTable, Errors errors, SchemaValidationContext context) {

    String tableName = assetTable.getName();
    List<String> columnNames = assetTable.getColumns();
    if (tableName != null && columnNames != null) {
      // An empty list acts like a wildcard to include all columns from a table in the asset
      // specification.
      if (columnNames.size() == 0) {
        if (!context.isValidTable(tableName)) {
          errors.rejectValue("schema", "InvalidAssetTable", "Invalid asset table: " + tableName);
        }
      } else {
        columnNames.forEach(
            (columnName) -> {
              if (!context.isValidTableColumn(tableName, columnName)) {
                errors.rejectValue(
                    "schema",
                    "InvalidAssetTableColumn",
                    "Invalid asset table: " + tableName + " column: " + columnName);
              }
            });
      }
    }
  }

  private void validateAsset(AssetModel asset, Errors errors, SchemaValidationContext context) {
    List<AssetTableModel> assetTables = asset.getTables();

    String rootTable = asset.getRootTable();
    String rootColumn = asset.getRootColumn();

    if (assetTables != null) {
      boolean hasRootTable = false;
      for (AssetTableModel assetTable : assetTables) {
        validateAssetTable(assetTable, errors, context);
        if (assetTable.getName().equals(rootTable)) {
          if (!context.isValidTableColumn(rootTable, rootColumn)) {
            errors.rejectValue(
                "schema",
                "InvalidRootColumn",
                "Invalid root table column. Table: " + rootTable + " Column: " + rootColumn);
          } else if (context.isArrayColumn(rootTable, rootColumn)) {
            errors.rejectValue(
                "schema",
                "InvalidArrayRootColumn",
                "Invalid use of array column as asset root. Table: "
                    + rootTable
                    + " Column: "
                    + rootColumn);
          }
          hasRootTable = true;
        }
      }
      if (!hasRootTable) {
        errors.rejectValue("schema", "NoRootTable");
      }
    }

    List<String> follows = asset.getFollow();
    if (follows != null) {
      if (follows.stream()
          .anyMatch(relationshipName -> !context.isValidRelationship(relationshipName))) {
        errors.rejectValue("schema", "InvalidFollowsRelationship");
      }
    }
    // TODO: There is another validation that can be done here to make sure that the graph is
    // connected that has
    // been left out to avoid complexity before we know if we're going keep using this or not.
  }

  private void validateSchema(
      DatasetSpecificationModel schema, CloudPlatformWrapper cloudPlatformWrapper, Errors errors) {
    SchemaValidationContext context = new SchemaValidationContext();
    List<TableModel> tables = schema.getTables();
    if (tables.isEmpty()) {
      errors.rejectValue(
          "schema", "IncompleteSchemaDefinition", "Dataset tables must be defined in the schema");
    } else {
      List<String> tableNames =
          tables.stream().map(TableModel::getName).collect(Collectors.toList());
      if (ValidationUtils.hasDuplicates(tableNames)) {
        errors.rejectValue("schema", "DuplicateTableNames");
      }
      tables.forEach((table) -> validateTable(table, errors, context));
    }

    List<RelationshipModel> relationships = schema.getRelationships();
    if (relationships != null) {
      List<String> relationshipNames =
          relationships.stream().map(RelationshipModel::getName).collect(Collectors.toList());
      if (ValidationUtils.hasDuplicates(relationshipNames)) {
        errors.rejectValue("schema", "DuplicateRelationshipNames");
      }
      relationships.forEach(
          (relationship) ->
              validateRelationship(relationship, tables, errors, context, cloudPlatformWrapper));
    }

    List<AssetModel> assets = schema.getAssets();
    if (assets != null) {
      List<String> assetNames =
          assets.stream().map(AssetModel::getName).collect(Collectors.toList());
      if (ValidationUtils.hasDuplicates(assetNames)) {
        List<String> duplicates = ValidationUtils.findDuplicates(assetNames);
        errors.rejectValue(
            "schema",
            "DuplicateAssetNames",
            String.format("Duplicate asset names: %s", String.join(", ", duplicates)));
      }
      assets.forEach((asset) -> validateAsset(asset, errors, context));
    }
  }

  private void validateRegion(
      DatasetRequestModel datasetRequest, CloudPlatformWrapper cloudWrapper, Errors errors) {
    if (datasetRequest.getRegion() != null) {
      cloudWrapper.ensureValidRegion(datasetRequest.getRegion(), errors);
    }
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target != null && target instanceof DatasetRequestModel) {
      DatasetRequestModel datasetRequest = (DatasetRequestModel) target;
      validateDatasetName(datasetRequest.getName(), errors);
      DatasetSpecificationModel schema = datasetRequest.getSchema();
      if (datasetRequest.getCloudPlatform() == null) {
        errors.rejectValue(
            "cloudPlatform", "InvalidCloudPlatform", "cloudPlatform must be provided.");
      } else {
        CloudPlatformWrapper cloudWrapper =
            CloudPlatformWrapper.of(datasetRequest.getCloudPlatform());
        if (schema != null) {
          validateSchema(schema, cloudWrapper, errors);
        }
        validateRegion(datasetRequest, cloudWrapper, errors);
      }
    }
  }
}
