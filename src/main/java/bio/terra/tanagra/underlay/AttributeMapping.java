package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.serialization.UFAttributeMapping;
import bio.terra.tanagra.underlay.displayhint.EnumVals;
import bio.terra.tanagra.underlay.displayhint.NumericRange;
import java.util.List;
import javax.annotation.Nullable;

public final class AttributeMapping {
  private static final String DEFAULT_DISPLAY_MAPPING_PREFIX = "t_display_";

  private final FieldPointer value;
  private final FieldPointer display;
  private Attribute attribute;

  public AttributeMapping(FieldPointer value) {
    this.value = value;
    this.display = null;
  }

  private AttributeMapping(FieldPointer value, FieldPointer display) {
    this.value = value;
    this.display = display;
  }

  public void initialize(Attribute attribute) {
    this.attribute = attribute;
  }

  public static AttributeMapping fromSerialized(
      @Nullable UFAttributeMapping serialized, TablePointer tablePointer, Attribute attribute) {
    // if the value is defined, then deserialize it
    // otherwise generate a default attribute mapping: a column with the same name as the attribute
    FieldPointer value =
        (serialized != null && serialized.getValue() != null)
            ? FieldPointer.fromSerialized(serialized.getValue(), tablePointer)
            : new FieldPointer.Builder()
                .tablePointer(tablePointer)
                .columnName(attribute.getName())
                .build();

    switch (attribute.getType()) {
      case SIMPLE:
        return new AttributeMapping(value);
      case KEY_AND_DISPLAY:
        // if the display is defined, then deserialize it
        // otherwise, generate a default attribute display mapping: a column with the same name as
        // the attribute with a prefix
        FieldPointer display =
            (serialized != null && serialized.getDisplay() != null)
                ? FieldPointer.fromSerialized(serialized.getDisplay(), tablePointer)
                    .setJoinCanBeEmpty(true) // Allow null display names.
                : new FieldPointer.Builder()
                    .tablePointer(tablePointer)
                    .columnName(DEFAULT_DISPLAY_MAPPING_PREFIX + attribute.getName())
                    .build();
        return new AttributeMapping(value, display);
      default:
        throw new InvalidConfigException("Attribute type is not defined");
    }
  }

  /**
   * @param tableVariables If this is a KEY_AND_DISPLAY attribute, the foreign table will appended
   *     to tableVariables. So tableVariables must be mutable: "Lists.newArrayList(...)", not "new
   *     List.of(...)".
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

  public Literal.DataType computeDataType() {
    DataPointer dataPointer = value.getTablePointer().getDataPointer();
    return dataPointer.lookupDatatype(value);
  }

  public DisplayHint computeDisplayHint() {
    if (attribute.getType().equals(Attribute.Type.KEY_AND_DISPLAY)) {
      return EnumVals.computeForField(attribute.getDataType(), value, display);
    }

    switch (attribute.getDataType()) {
      case BOOLEAN:
        return null; // boolean values are enum by default
      case INT64:
        return NumericRange.computeForField(value);
      case STRING:
        return EnumVals.computeForField(attribute.getDataType(), value);
      case DATE:
      case DOUBLE:
        // TODO: Compute display hints for other data types.
        return null;
      default:
        throw new InvalidConfigException("Unknown attribute data type: " + attribute.getDataType());
    }
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
