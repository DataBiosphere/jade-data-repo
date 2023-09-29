package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.TablePointer;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public record AuxiliaryDataMapping(TablePointer tablePointer,
                                   Map<String, FieldPointer> fieldPointers) {

  /**
   * Build a default auxiliary data mapping to a table with the same name as the auxiliary data
   * object (plus a prefix), and columns with the same name as the auxiliary data fields.
   */
  public static AuxiliaryDataMapping defaultIndexMapping(
      AuxiliaryData auxiliaryData, String tablePrefix, DataPointer dataPointer) {
    TablePointer table =
        TablePointer.fromTableName(dataPointer, tablePrefix + auxiliaryData.getName());
    return new AuxiliaryDataMapping(
        table,
        auxiliaryData.getFields().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    fieldName ->
                        new FieldPointer.Builder()
                            .tablePointer(table)
                            .columnName(fieldName)
                            .build())));
  }

  @Override
  public Map<String, FieldPointer> fieldPointers() {
    return Map.copyOf(fieldPointers);
  }
}
