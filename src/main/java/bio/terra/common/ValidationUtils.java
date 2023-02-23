package bio.terra.common;

import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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

  // Following rules for implicit conversion as defined here:
  // https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-type-conversion-database-engine?view=sql-server-ver16
  private boolean compatibleDataType(TableDataType fromDataType, TableDataType toDataType) {
    switch(fromDataType) {
      case BOOLEAN:
      case FLOAT:
      case FLOAT64:
      case INTEGER:
      case INT64:
      case NUMERIC:
        return List.of(TableDataType.BOOLEAN, TableDataType.BYTES, TableDataType.FLOAT, TableDataType.FLOAT64, TableDataType.INTEGER, TableDataType.INT64, TableDataType.NUMERIC, TableDataType.TEXT, TableDataType.STRING).contains(toDataType);
      case BYTES:
        return List.of(TableDataType.BOOLEAN, TableDataType.BYTES, TableDataType.INTEGER, TableDataType.INT64,  TableDataType.TEXT, TableDataType.STRING).contains(toDataType);
      case DATE:
        return List.of(TableDataType.DATE, TableDataType.DATETIME, TableDataType.TIMESTAMP, TableDataType.STRING, TableDataType.TEXT).contains(toDataType);
      case DATETIME:
      case TIMESTAMP:
        return List.of(TableDataType.DATE, TableDataType.DATETIME, TableDataType.TIMESTAMP, TableDataType.STRING, TableDataType.TEXT, TableDataType.TIME).contains(toDataType);
      case DIRREF:
      case FILEREF:
        return List.of(TableDataType.DIRREF, TableDataType.FILEREF).contains(toDataType);
      case TEXT:
      case STRING:
        return List.of(TableDataType.BOOLEAN, TableDataType.BYTES, TableDataType.FLOAT, TableDataType.FLOAT64, TableDataType.INTEGER, TableDataType.INT64, TableDataType.NUMERIC, TableDataType.TEXT, TableDataType.STRING, TableDataType.DATE, TableDataType.DATETIME, TableDataType.TIMESTAMP, TableDataType.TIME).contains(toDataType);
      case TIME:
        return List.of(TableDataType.DATETIME, TableDataType.TIMESTAMP, TableDataType.STRING, TableDataType.TEXT, TableDataType.TIME).contains(toDataType);
      default:
        return false;
    }
  }

  public static LinkedHashMap<String, String> validateMatchingColumnDataTypes(
      RelationshipTermModel fromTerm, RelationshipTermModel toTerm, List<TableModel> tables) {
    LinkedHashMap<String, String> termErrors = new LinkedHashMap<>();
    Optional<ColumnModel> fromColumn = retrieveColumnModelFromTerm(fromTerm, tables);
    Optional<ColumnModel> toColumn = retrieveColumnModelFromTerm(toTerm, tables);
    if (fromColumn.isPresent() && toColumn.isPresent()) {
      TableDataType fromColumnDataType = fromColumn.get().getDatatype();
      TableDataType toColumnDataType = toColumn.get().getDatatype();
      if (!fromColumnDataType.equals(toColumnDataType)) {
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
    }
    return termErrors;
  }

  public static LinkedHashMap<String, String> validateRelationshipTerm(
      RelationshipTermModel term, List<TableModel> tables) {
    String tableName = term.getTable();
    String columnName = term.getColumn();
    LinkedHashMap<String, String> termErrors = new LinkedHashMap<>();
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

  private static Optional<ColumnModel> retrieveColumnModelFromTerm(
      RelationshipTermModel term, List<TableModel> tables) {
    String tableName = term.getTable();
    String columnName = term.getColumn();
    try {
      return tables.stream()
          .filter(t -> t.getName().equals(tableName))
          .findFirst()
          .get()
          .getColumns()
          .stream()
          .filter(c -> c.getName().equals(columnName))
          .findFirst();
    } catch (NoSuchElementException ex) {
      return Optional.empty();
    }
  }

  public static ArrayList<LinkedHashMap<String, String>> getRelationshipValidationErrors(
      RelationshipModel relationship, List<TableModel> tables) {
    ArrayList<LinkedHashMap<String, String>> errors = new ArrayList<>();
    RelationshipTermModel fromTerm = relationship.getFrom();
    if (fromTerm != null) {
      errors.add(validateRelationshipTerm(fromTerm, tables));
    }

    RelationshipTermModel toTerm = relationship.getTo();
    if (toTerm != null) {
      errors.add(validateRelationshipTerm(toTerm, tables));
    }

    if (fromTerm != null && toTerm != null) {
      validateMatchingColumnDataTypes(fromTerm, toTerm, tables);
    }

    return errors;
  }

  public static boolean isInvalidPrimaryOrForeignKeyType(ColumnModel columnModel) {
    Set<TableDataType> invalidTypes = Set.of(TableDataType.DIRREF, TableDataType.FILEREF);
    return columnModel.getDatatype() != null && invalidTypes.contains(columnModel.getDatatype());
  }
}
