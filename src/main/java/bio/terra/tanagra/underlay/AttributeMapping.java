package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.azure.AzureExecutor;
import bio.terra.tanagra.underlay.displayhint.EnumVals;
import bio.terra.tanagra.underlay.displayhint.NumericRange;
import java.util.List;

public final class AttributeMapping {
  private static final String DEFAULT_DISPLAY_MAPPING_PREFIX = "t_display_";

  private final FieldPointer value;
  private final FieldPointer display;
  private Attribute attribute;

  public AttributeMapping(FieldPointer value) {
    this(value, null);
  }

  private AttributeMapping(FieldPointer value, FieldPointer display) {
    this.value = value;
    this.display = display;
  }

  public void initialize(Attribute attribute) {
    this.attribute = attribute;
  }

  /**
   * @param tableVariables If this is a KEY_AND_DISPLAY attribute, the foreign table will be
   *     appended to tableVariables. So tableVariables must be mutable: "Lists.newArrayList(...)",
   *     not "new List.of(...)".
   */
  public List<FieldVariable> buildFieldVariables(
      TableVariable primaryTable, List<TableVariable> tableVariables) {
    FieldVariable valueVariable =
        value.buildVariable(primaryTable, tableVariables, attribute.getName());
    if (!hasDisplay()) {
      return List.of(valueVariable);
    }

    FieldVariable displayVariable =
        display.buildVariable(primaryTable, tableVariables, getDisplayMappingAlias());
    return List.of(valueVariable, displayVariable);
  }

  public ColumnSchema buildValueColumnSchema() {
    return new ColumnSchema(
        attribute.getName(), CellValue.SQLDataType.fromUnderlayDataType(attribute.getDataType()));
  }

  public List<ColumnSchema> buildColumnSchemas() {
    ColumnSchema valueColSchema = buildValueColumnSchema();
    if (!hasDisplay()) {
      return List.of(valueColSchema);
    }

    ColumnSchema displayColSchema =
        new ColumnSchema(getDisplayMappingAlias(), CellValue.SQLDataType.STRING);
    return List.of(valueColSchema, displayColSchema);
  }

  public String getDisplayMappingAlias() {
    return DEFAULT_DISPLAY_MAPPING_PREFIX + attribute.getName();
  }

  public Literal.DataType computeDataType(AzureExecutor executor) {
    DataPointer dataPointer = value.getTablePointer().dataPointer();
    return dataPointer.lookupDatatype(value, executor);
  }

  public DisplayHint computeDisplayHint(QueryExecutor executor) {
    if (attribute.getType() == Attribute.Type.KEY_AND_DISPLAY) {
      return EnumVals.computeForField(attribute.getDataType(), value, display, executor);
    }

    return switch (attribute.getDataType()) {
      case BOOLEAN -> null; // boolean values are enum by default
      case INT64 -> NumericRange.computeForField(value, executor);
      case STRING -> EnumVals.computeForField(attribute.getDataType(), value, executor);
      case DATE, DOUBLE ->
      // TODO: Compute display hints for other data types.
      null;
    };
  }

  public List<FieldPointer> getFieldPointers() {
    if (hasDisplay()) {
      return List.of(value, display);
    } else {
      return List.of(value);
    }
  }

  public boolean hasDisplay() {
    return display != null;
  }

  public FieldPointer getValue() {
    return value;
  }

  public FieldPointer getDisplay() {
    return display;
  }
}
