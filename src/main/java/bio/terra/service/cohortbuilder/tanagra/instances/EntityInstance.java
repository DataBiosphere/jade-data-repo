package bio.terra.service.cohortbuilder.tanagra.instances;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.RelationshipField;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.ValueDisplay;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class EntityInstance {
  private final Map<Attribute, ValueDisplay> attributeValues;
  private final Map<HierarchyField, ValueDisplay> hierarchyFieldValues;
  private final Map<RelationshipField, ValueDisplay> relationshipFieldValues;

  private EntityInstance(
      Map<Attribute, ValueDisplay> attributeValues,
      Map<HierarchyField, ValueDisplay> hierarchyFieldValues,
      Map<RelationshipField, ValueDisplay> relationshipFieldValues) {
    this.attributeValues = attributeValues;
    this.hierarchyFieldValues = hierarchyFieldValues;
    this.relationshipFieldValues = relationshipFieldValues;
  }

  public static EntityInstance fromRowResult(
      RowResult rowResult,
      List<Attribute> selectedAttributes,
      List<HierarchyField> selectedHierarchyFields,
      List<RelationshipField> selectedRelationshipFields) {
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

    Map<HierarchyField, ValueDisplay> hierarchyFieldValues = new HashMap<>();
    for (HierarchyField selectedHierarchyField : selectedHierarchyFields) {
      CellValue cellValue = rowResult.get(selectedHierarchyField.getFieldAlias());
      if (cellValue == null) {
        throw new SystemException(
            "Hierarchy field column not found: "
                + selectedHierarchyField.getHierarchy().getName()
                + ", "
                + selectedHierarchyField.getType());
      }
      Optional<Literal> valueOpt = cellValue.getLiteral();
      if (valueOpt.isEmpty()) {
        hierarchyFieldValues.put(selectedHierarchyField, null);
      } else {
        hierarchyFieldValues.put(selectedHierarchyField, new ValueDisplay(valueOpt.get()));
      }
    }

    Map<RelationshipField, ValueDisplay> relationshipFieldValues = new HashMap<>();
    for (RelationshipField selectedRelationshipField : selectedRelationshipFields) {
      CellValue cellValue = rowResult.get(selectedRelationshipField.getFieldAlias());
      if (cellValue == null) {
        throw new SystemException(
            "Relationship field column not found: "
                + selectedRelationshipField.getEntity().getName()
                + ", "
                + selectedRelationshipField.getHierarchyName()
                + ", "
                + selectedRelationshipField.getType());
      }
      Optional<Literal> valueOpt = cellValue.getLiteral();
      if (valueOpt.isEmpty()) {
        relationshipFieldValues.put(selectedRelationshipField, null);
      } else {
        relationshipFieldValues.put(selectedRelationshipField, new ValueDisplay(valueOpt.get()));
      }
    }

    return new EntityInstance(attributeValues, hierarchyFieldValues, relationshipFieldValues);
  }

  public Map<Attribute, ValueDisplay> getAttributeValues() {
    return Collections.unmodifiableMap(attributeValues);
  }

  public Map<HierarchyField, ValueDisplay> getHierarchyFieldValues() {
    return Collections.unmodifiableMap(hierarchyFieldValues);
  }

  public Map<RelationshipField, ValueDisplay> getRelationshipFieldValues() {
    return Collections.unmodifiableMap(relationshipFieldValues);
  }
}
