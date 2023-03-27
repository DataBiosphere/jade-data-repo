package bio.terra.tanagra.service.instances;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.ValueDisplay;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class EntityInstanceCount {
  public static final String DEFAULT_COUNT_COLUMN_NAME = "t_count";

  private final long count;
  private final Map<Attribute, ValueDisplay> attributeValues;

  private EntityInstanceCount(long count, Map<Attribute, ValueDisplay> attributeValues) {
    this.count = count;
    this.attributeValues = attributeValues;
  }

  public static EntityInstanceCount fromRowResult(
      RowResult rowResult, List<Attribute> selectedAttributes) {
    CellValue countCellValue = rowResult.get(DEFAULT_COUNT_COLUMN_NAME);
    if (countCellValue == null) {
      throw new SystemException("Count column not found: " + DEFAULT_COUNT_COLUMN_NAME);
    }
    long count = countCellValue.getLiteral().orElseThrow().getInt64Val();

    Map<Attribute, ValueDisplay> attributeValues = new HashMap<>();
    for (Attribute selectedAttribute : selectedAttributes) {
      CellValue cellValue = rowResult.get(selectedAttribute.getName());
      if (cellValue == null) {
        throw new SystemException("Attribute column not found: " + selectedAttribute.getName());
      }

      Optional<Literal> valueOpt = cellValue.getLiteral();
      if (valueOpt.isEmpty()) {
        attributeValues.put(selectedAttribute, null);
      } else if (selectedAttribute.getType() == Attribute.Type.SIMPLE) {
        attributeValues.put(selectedAttribute, new ValueDisplay(valueOpt.get()));
      } else if (selectedAttribute.getType() == Attribute.Type.KEY_AND_DISPLAY) {
        String display =
            rowResult
                .get(
                    selectedAttribute
                        .getMapping(Underlay.MappingType.INDEX)
                        .getDisplayMappingAlias())
                .getString()
                .get();
        attributeValues.put(selectedAttribute, new ValueDisplay(valueOpt.get(), display));
      } else {
        throw new SystemException("Unknown attribute type: " + selectedAttribute.getType());
      }
    }

    return new EntityInstanceCount(count, attributeValues);
  }

  public long getCount() {
    return count;
  }

  public Map<Attribute, ValueDisplay> getAttributeValues() {
    return Collections.unmodifiableMap(attributeValues);
  }
}
