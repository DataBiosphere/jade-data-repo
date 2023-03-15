package bio.terra.common;

import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class ValidationUtils {

  // pattern taken from https://stackoverflow.com/questions/8204680/java-regex-email
  private static final Pattern VALID_EMAIL_REGEX =
      Pattern.compile(
          "[a-z0-9!#$%&'*+/=?^_`{|}~-]+"
              + "(?:.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?",
          Pattern.CASE_INSENSITIVE);

  // path needs to start with a leading forward slash
  // TODO: This validation check is NOT required by the underlying code. The filesystem dao is
  // more forgiving. Should we enforce this or fix it up, as is done in filesystem.
  private static final String VALID_PATH = "/.*";

  private ValidationUtils() {}

  public static <T> boolean hasDuplicates(List<T> list) {
    return !(list.size() == new HashSet(list).size());
  }

  public static <T> List<T> findDuplicates(List<T> list) {
    return list.stream()
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        .entrySet()
        .stream()
        .filter(e -> e.getValue() > 1)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  public static boolean isValidDescription(String name) {
    return name.length() < 2048;
  }

  public static boolean isValidEmail(String email) {
    return VALID_EMAIL_REGEX.matcher(email).matches();
  }

  public static boolean isValidPath(String path) {
    return Pattern.matches(VALID_PATH, path);
  }

  public static boolean isValidUuid(String uuid) {
    return convertToUuid(uuid).isPresent();
  }

  public static Optional<UUID> convertToUuid(String uuid) {
    try {
      return Optional.of(UUID.fromString(uuid));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  public static String requireNotBlank(String value, String errorMsg) {
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(errorMsg);
    }

    return value;
  }

  @VisibleForTesting
  static boolean isCompatibleDataType(
      TableDataType fromDataType,
      TableDataType toDataType,
      CloudPlatformWrapper cloudPlatformWrapper) {
    List<TableDataType> compatibleTypes = null;
    if (cloudPlatformWrapper.isGcp()) {
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules
      compatibleTypes =
          switch (fromDataType) {
            case DATE, DATETIME -> List.of(TableDataType.DATE, TableDataType.DATETIME);
            case DIRREF, FILEREF -> List.of(TableDataType.DIRREF, TableDataType.FILEREF);
            case FLOAT, FLOAT64, INTEGER, INT64, NUMERIC -> List.of(
                TableDataType.FLOAT,
                TableDataType.FLOAT64,
                TableDataType.INTEGER,
                TableDataType.INT64,
                TableDataType.NUMERIC);
            case STRING, TEXT -> List.of(TableDataType.STRING, TableDataType.TEXT);
            default -> List.of(fromDataType);
          };
    } else if (cloudPlatformWrapper.isAzure()) {
      // Following rules for implicit conversion in both directions as defined here:
      // https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-type-conversion-database-engine?view=sql-server-ver16
      compatibleTypes =
          switch (fromDataType) {
            case BOOLEAN, FLOAT, FLOAT64, INTEGER, INT64, NUMERIC -> List.of(
                TableDataType.BOOLEAN,
                TableDataType.FLOAT,
                TableDataType.FLOAT64,
                TableDataType.TEXT,
                TableDataType.STRING,
                TableDataType.INTEGER,
                TableDataType.INT64,
                TableDataType.NUMERIC);
            case BYTES -> List.of(
                TableDataType.BOOLEAN,
                TableDataType.BYTES,
                TableDataType.INTEGER,
                TableDataType.INT64,
                TableDataType.NUMERIC);
            case DATE -> List.of(
                TableDataType.DATE,
                TableDataType.DATETIME,
                TableDataType.TIMESTAMP,
                TableDataType.TEXT,
                TableDataType.STRING);
            case DATETIME, TIMESTAMP -> List.of(
                TableDataType.DATE,
                TableDataType.DATETIME,
                TableDataType.TIMESTAMP,
                TableDataType.TIME,
                TableDataType.TEXT,
                TableDataType.STRING);
            case TIME -> List.of(
                TableDataType.DATETIME,
                TableDataType.TIMESTAMP,
                TableDataType.TEXT,
                TableDataType.STRING);
            case DIRREF, FILEREF -> List.of(TableDataType.DIRREF, TableDataType.FILEREF);
            case TEXT, STRING -> List.of(
                TableDataType.BOOLEAN,
                TableDataType.BYTES,
                TableDataType.DATE,
                TableDataType.DATETIME,
                TableDataType.TIMESTAMP,
                TableDataType.FLOAT,
                TableDataType.FLOAT64,
                TableDataType.INTEGER,
                TableDataType.INT64,
                TableDataType.NUMERIC,
                TableDataType.TEXT,
                TableDataType.STRING,
                TableDataType.TIME);
            default -> List.of(fromDataType);
          };
    } else {
      compatibleTypes = List.of();
    }
    return compatibleTypes.contains(toDataType);
  }

  public static Map<String, String> validateMatchingColumnDataTypes(
      RelationshipTermModel fromTerm,
      RelationshipTermModel toTerm,
      List<TableModel> tables,
      CloudPlatformWrapper cloudPlatformWrapper) {
    Map<String, String> termErrors = new HashMap<>();
    retrieveColumnModelFromTerm(fromTerm, tables)
        .ifPresent(
            fromColumn ->
                retrieveColumnModelFromTerm(toTerm, tables)
                    .ifPresent(
                        toColumn -> {
                          TableDataType fromColumnDataType = fromColumn.getDatatype();
                          TableDataType toColumnDataType = toColumn.getDatatype();
                          if (!isCompatibleDataType(
                              fromColumnDataType, toColumnDataType, cloudPlatformWrapper)) {
                            termErrors.put(
                                "RelationshipDatatypeMismatch",
                                String.format(
                                    "Column data types in relationship must match: Column %s.%s has data type %s and Column %s.%s has data type %s",
                                    fromTerm.getTable(),
                                    fromTerm.getColumn(),
                                    fromColumnDataType,
                                    toTerm.getTable(),
                                    toTerm.getColumn(),
                                    toColumnDataType));
                          }
                        }));
    return termErrors;
  }

  public static Map<String, String> validateRelationshipTerm(
      RelationshipTermModel term, List<TableModel> tables) {
    String tableName = term.getTable();
    String columnName = term.getColumn();
    Map<String, String> termErrors = new HashMap<>();
    Optional<TableModel> table =
        tables.stream().filter(t -> t.getName().equals(tableName)).findFirst();
    if (table.isEmpty()) {
      termErrors.put("InvalidRelationshipTermTable", String.format("Invalid table %s", tableName));
    } else {
      Optional<ColumnModel> columnModel = retrieveColumnModelFromTerm(term, tables);
      if (columnModel.isEmpty()) {
        termErrors.put(
            "InvalidRelationshipTermTableColumn",
            String.format("Invalid column %s.%s", tableName, columnName));
      } else {
        if (isInvalidPrimaryOrForeignKeyType(columnModel.get())) {
          termErrors.put(
              "InvalidRelationshipColumnType",
              String.format(
                  "Relationship column %s cannot be %s type",
                  columnName, columnModel.get().getDatatype()));
        }
      }
    }
    return termErrors;
  }

  @VisibleForTesting
  static Optional<ColumnModel> retrieveColumnModelFromTerm(
      RelationshipTermModel term, List<TableModel> tables) {
    String tableName = term.getTable();
    String columnName = term.getColumn();
    return tables.stream()
        .filter(t -> t.getName().equals(tableName))
        .findFirst()
        .flatMap(
            tableModel ->
                tableModel.getColumns().stream()
                    .filter(c -> c.getName().equals(columnName))
                    .findFirst());
  }

  public static List<Map<String, String>> getRelationshipValidationErrors(
      RelationshipModel relationship,
      List<TableModel> tables,
      CloudPlatformWrapper cloudPlatformWrapper) {
    List<Map<String, String>> errors = new ArrayList<>();
    RelationshipTermModel fromTerm = relationship.getFrom();
    if (fromTerm != null) {
      errors.add(validateRelationshipTerm(fromTerm, tables));
    }

    RelationshipTermModel toTerm = relationship.getTo();
    if (toTerm != null) {
      errors.add(validateRelationshipTerm(toTerm, tables));
    }

    if (fromTerm != null && toTerm != null) {
      errors.add(validateMatchingColumnDataTypes(fromTerm, toTerm, tables, cloudPlatformWrapper));
    }

    return errors;
  }

  public static boolean isInvalidPrimaryOrForeignKeyType(ColumnModel columnModel) {
    Set<TableDataType> invalidTypes = Set.of(TableDataType.DIRREF, TableDataType.FILEREF);
    return columnModel.getDatatype() != null && invalidTypes.contains(columnModel.getDatatype());
  }
}
