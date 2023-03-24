package bio.terra.service.cohortbuilder.tanagra.utils;

import bio.terra.model.Attribute;
import bio.terra.model.Cohort;
import bio.terra.model.Criteria;
import bio.terra.model.CriteriaGroup;
import bio.terra.model.DataType;
import bio.terra.model.InstanceCount;
import bio.terra.model.Literal;
import bio.terra.model.LiteralValueUnion;
import bio.terra.model.ValueDisplay;
import bio.terra.service.cohortbuilder.tanagra.instances.EntityInstanceCount;
import bio.terra.tanagra.exception.SystemException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class ToApiConversionUtils {
  private ToApiConversionUtils() {}

  public static Attribute toApiObject(bio.terra.tanagra.underlay.Attribute attribute) {
    return new Attribute()
        .name(attribute.getName())
        .type(Attribute.TypeEnum.fromValue(attribute.getType().name()))
        .dataType(DataType.fromValue(attribute.getDataType().name()));
  }

  public static ValueDisplay toApiObject(bio.terra.tanagra.underlay.ValueDisplay valueDisplay) {
    ValueDisplay apiObject = new ValueDisplay();
    if (valueDisplay != null) {
      apiObject.value(toApiObject(valueDisplay.getValue())).display(valueDisplay.getDisplay());
    }
    return apiObject;
  }

  public static Literal toApiObject(bio.terra.tanagra.query.Literal literal) {
    Literal apiLiteral = new Literal().dataType(DataType.fromValue(literal.getDataType().name()));
    switch (literal.getDataType()) {
      case INT64:
        return apiLiteral.valueUnion(new LiteralValueUnion().int64Val(literal.getInt64Val()));
      case STRING:
        return apiLiteral.valueUnion(new LiteralValueUnion().stringVal(literal.getStringVal()));
      case BOOLEAN:
        return apiLiteral.valueUnion(new LiteralValueUnion().boolVal(literal.getBooleanVal()));
      case DATE:
        return apiLiteral.valueUnion(new LiteralValueUnion().dateVal(literal.getDateValAsString()));
      default:
        throw new SystemException("Unknown literal data type: " + literal.getDataType());
    }
  }

  /**
   * Convert the internal Cohort object to an API Cohort object.
   *
   * <p>In the backend code, a Cohort = a filter on the primary entity, and a CohortRevisionGroup =
   * all past versions and the current version of a filter on the primary entity.
   */
  public static Cohort toApiObject(bio.terra.service.cohortbuilder.tanagra.artifact.Cohort cohort) {
    return new Cohort()
        .id(cohort.getCohortRevisionGroupId())
        .displayName(cohort.getDisplayName())
        .description(cohort.getDescription())
        .created(cohort.getCreated())
        .createdBy(cohort.getCreatedBy())
        .lastModified(cohort.getLastModified())
        .criteriaGroups(
            cohort.getCriteriaGroups().stream()
                .map(criteriaGroup -> toApiObject(criteriaGroup))
                .collect(Collectors.toList()));
  }

  private static CriteriaGroup toApiObject(
      bio.terra.service.cohortbuilder.tanagra.artifact.CriteriaGroup criteriaGroup) {
    return new CriteriaGroup()
        .id(criteriaGroup.getUserFacingCriteriaGroupId())
        .displayName(criteriaGroup.getDisplayName())
        .operator(CriteriaGroup.OperatorEnum.fromValue(criteriaGroup.getOperator().name()))
        .excluded(criteriaGroup.isExcluded())
        .criteria(
            criteriaGroup.getCriterias().stream()
                .map(criteria -> toApiObject(criteria))
                .collect(Collectors.toList()));
  }

  public static Criteria toApiObject(
      bio.terra.service.cohortbuilder.tanagra.artifact.Criteria criteria) {
    return new Criteria()
        .id(criteria.getUserFacingCriteriaId())
        .displayName(criteria.getDisplayName())
        .pluginName(criteria.getPluginName())
        .selectionData(criteria.getSelectionData())
        .uiConfig(criteria.getUiConfig());
  }

  public static InstanceCount toApiObject(EntityInstanceCount entityInstanceCount) {
    InstanceCount instanceCount = new InstanceCount();
    Map<String, ValueDisplay> attributes = new HashMap<>();
    for (Map.Entry<bio.terra.tanagra.underlay.Attribute, bio.terra.tanagra.underlay.ValueDisplay>
        attributeValue : entityInstanceCount.getAttributeValues().entrySet()) {
      attributes.put(attributeValue.getKey().getName(), toApiObject(attributeValue.getValue()));
    }

    return instanceCount
        .count(Math.toIntExact(entityInstanceCount.getCount()))
        .attributes(attributes);
  }
}
