package bio.terra.service.dataset;

import bio.terra.common.PdaoConstant;
import bio.terra.common.ValidationUtils;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatePartitionOptionsModel;
import bio.terra.model.IntPartitionOptionsModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.validation.Errors;

/**
 * SchemaValidationContext represents shared functionality between DatasetRequestValidator (used for
 * creating dataset schemas) and DatasetSchemaUpdateValidator (used for updating dataset schemas).
 */
public class SchemaValidationContext {

  private static final String PRIMARY_KEY = "PrimaryKey";

  enum Operation {
    CREATE("schema"),
    UPDATE("changes");

    private final String fieldName;

    Operation(String fieldName) {
      this.fieldName = fieldName;
    }
  }

  private final HashMap<String, HashSet<String>> tableColumnMap;
  private final HashMap<String, HashSet<String>> tableArrayColumns;
  private final HashSet<String> relationshipNameSet;
  private final String fieldName;

  SchemaValidationContext(String field) {
    tableColumnMap = new HashMap<>();
    tableArrayColumns = new HashMap<>();
    relationshipNameSet = new HashSet<>();
    fieldName = field;
  }

  static SchemaValidationContext forUpdate() {
    return new SchemaValidationContext("changes");
  }

  static SchemaValidationContext forCreate() {
    return new SchemaValidationContext("schema");
  }

  String getFieldName() {
    return fieldName;
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

  public void validateTable(TableModel table, Errors errors) {
    String tableName = table.getName();
    List<ColumnModel> columns = table.getColumns();
    List<String> primaryKeyList = table.getPrimaryKey();
    List<String> columnNames = new ArrayList<>();
    if (columns.isEmpty()) {
      errors.rejectValue(
          getFieldName(),
          "IncompleteSchemaDefinition",
          "Each table must contain at least one column");
    } else {
      columns.stream().map(ColumnModel::getName).forEach(columnNames::add);
    }

    if (tableName != null) {
      validateDataTypes(columns, errors);

      if (ValidationUtils.hasDuplicates(columnNames)) {
        List<String> duplicates = ValidationUtils.findDuplicates(columnNames);
        errors.rejectValue(
            getFieldName(),
            "DuplicateColumnNames",
            String.format("Duplicate columns: %s", String.join(", ", duplicates)));
      }
      if (primaryKeyList != null) {
        if (!columnNames.containsAll(primaryKeyList)) {
          List<String> missingKeys = new ArrayList<>(primaryKeyList);
          missingKeys.removeAll(columnNames);
          errors.rejectValue(
              getFieldName(),
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

      addTable(tableName, columns);
    }

    TableModel.PartitionModeEnum mode = table.getPartitionMode();
    DatePartitionOptionsModel dateOptions = table.getDatePartitionOptions();
    IntPartitionOptionsModel intOptions = table.getIntPartitionOptions();

    if (mode == TableModel.PartitionModeEnum.DATE) {
      if (dateOptions == null) {
        errors.rejectValue(
            getFieldName(),
            "MissingDatePartitionOptions",
            "datePartitionOptions must be specified when using 'date' partitionMode");
      } else {
        validateDatePartitionOptions(dateOptions, columns, errors);
      }
    } else if (dateOptions != null) {
      errors.rejectValue(
          getFieldName(),
          "InvalidDatePartitionOptions",
          "datePartitionOptions can only be specified when using 'date' partitionMode");
    }

    if (mode == TableModel.PartitionModeEnum.INT) {
      if (intOptions == null) {
        errors.rejectValue(
            getFieldName(),
            "MissingIntPartitionOptions",
            "intPartitionOptions must be specified when using 'int' partitionMode");
      } else {
        validateIntPartitionOptions(intOptions, columns, errors);
      }
    } else if (intOptions != null) {
      errors.rejectValue(
          getFieldName(),
          "InvalidIntPartitionOptions",
          "intPartitionOptions can only be specified when using 'int' partitionMode");
    }
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
          getFieldName(),
          "InvalidDatatype",
          "invalid datatype in table column(s): "
              + invalidColumns.stream().map(ColumnModel::getName).collect(Collectors.joining(", "))
              + ", DataTypes must be lowercase, valid DataTypes are "
              + Arrays.toString(TableDataType.values()));
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
          getFieldName(),
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
          getFieldName(),
          "InvalidColumnMode",
          String.format("Array column %s cannot be marked as required", columnModel.getName()));
    }
  }

  private void validateDatePartitionOptions(
      DatePartitionOptionsModel options, List<ColumnModel> columns, Errors errors) {
    String targetColumn = options.getColumn();

    if (targetColumn == null) {
      errors.rejectValue(getFieldName(), "MissingDatePartitionColumnName");
    } else if (!targetColumn.equals(PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS)) {
      Optional<ColumnModel> matchingColumn =
          columns.stream().filter(c -> targetColumn.equals(c.getName())).findFirst();

      if (matchingColumn.isPresent()) {
        TableDataType colType = matchingColumn.get().getDatatype();

        if (colType != TableDataType.DATE && colType != TableDataType.TIMESTAMP) {
          errors.rejectValue(
              getFieldName(),
              "InvalidDatePartitionColumnType",
              "partitionColumn in datePartitionOptions must refer to a DATE or TIMESTAMP column");
        }
      } else {
        errors.rejectValue(
            getFieldName(), "InvalidDatePartitionColumnName", "No such column: " + targetColumn);
      }
    }
  }

  private void validateIntPartitionOptions(
      IntPartitionOptionsModel options, List<ColumnModel> columns, Errors errors) {
    String targetColumn = options.getColumn();

    if (targetColumn == null) {
      errors.rejectValue(getFieldName(), "MissingIntPartitionColumnName");
    } else {
      Optional<ColumnModel> matchingColumn =
          columns.stream().filter(c -> targetColumn.equals(c.getName())).findFirst();

      if (matchingColumn.isPresent()) {
        TableDataType colType = matchingColumn.get().getDatatype();

        if (colType != TableDataType.INTEGER && colType != TableDataType.INT64) {
          errors.rejectValue(
              getFieldName(),
              "InvalidIntPartitionColumnType",
              "partitionColumn in intPartitionOptions must refer to an INTEGER or INT64 column");
        }
      } else {
        errors.rejectValue(
            getFieldName(), "InvalidIntPartitionColumnName", "No such column: " + targetColumn);
      }
    }

    Long min = options.getMin();
    Long max = options.getMax();
    Long interval = options.getInterval();

    if (min == null || max == null || interval == null) {
      errors.rejectValue(
          getFieldName(),
          "MissingIntPartitionOptions",
          "intPartitionOptions must specify min, max, and interval");
    } else {
      if (max <= min) {
        errors.rejectValue(
            getFieldName(),
            "InvalidIntPartitionRange",
            "Max partition value must be larger than min partition value");
      }
      if (interval <= 0) {
        errors.rejectValue(
            getFieldName(), "InvalidIntPartitionInterval", "Partition interval must be >= 1");
      }
      if (max > min && interval > 0 && (max - min) / interval > 4000L) {
        errors.rejectValue(
            getFieldName(),
            "TooManyIntPartitions",
            "Cannot configure more than 4K partitions through min, max, and interval");
      }
    }
  }

  private void rejectKey(Errors errors, String keyType, String columnName, String type) {
    errors.rejectValue(
        getFieldName(),
        String.format("Invalid%s", keyType),
        String.format("%s %s cannot be a column with %s type", keyType, columnName, type));
  }
}
