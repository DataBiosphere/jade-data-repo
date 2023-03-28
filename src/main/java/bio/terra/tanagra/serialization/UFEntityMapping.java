package bio.terra.tanagra.serialization;

import static bio.terra.tanagra.underlay.Underlay.MappingType.SOURCE;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.AGE_AT_OCCURRENCE_ATTRIBUTE_NAME;

import bio.terra.tanagra.underlay.EntityMapping;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.HashMap;
import java.util.Map;

/**
 * External representation of the data mapped to an entity.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFEntityMapping.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFEntityMapping {
  private final String dataPointer;
  private final UFTablePointer tablePointer;
  private final Map<String, UFAttributeMapping> attributeMappings;
  private final UFTextSearchMapping textSearchMapping;
  private final Map<String, UFHierarchyMapping> hierarchyMappings;

  public UFEntityMapping(EntityMapping entityMapping) {
    this.dataPointer = entityMapping.getTablePointer().getDataPointer().getName();
    this.tablePointer = new UFTablePointer(entityMapping.getTablePointer());

    Map<String, UFAttributeMapping> attributeMappings = new HashMap<>();
    entityMapping.getEntity().getAttributes().stream()
        .forEach(
            attribute -> {
              // Tanagra automatically generates age_at_occurrence attribute for some entities.
              // There is no source mapping.
              if (entityMapping.getMappingType() == SOURCE
                  && attribute.getName().equals(AGE_AT_OCCURRENCE_ATTRIBUTE_NAME)) {
                return;
              }
              attributeMappings.put(
                  attribute.getName(),
                  new UFAttributeMapping(attribute.getMapping(entityMapping.getMappingType())));
            });
    this.attributeMappings = attributeMappings;

    this.textSearchMapping =
        entityMapping.getEntity().getTextSearch().isEnabled()
            ? new UFTextSearchMapping(
                entityMapping
                    .getEntity()
                    .getTextSearch()
                    .getMapping(entityMapping.getMappingType()))
            : null;

    Map<String, UFHierarchyMapping> hierarchyMappings = new HashMap<>();
    entityMapping.getEntity().getHierarchies().stream()
        .forEach(
            hierarchy -> {
              hierarchyMappings.put(
                  hierarchy.getName(),
                  new UFHierarchyMapping(hierarchy.getMapping(entityMapping.getMappingType())));
            });
    this.hierarchyMappings = hierarchyMappings;
  }

  private UFEntityMapping(Builder builder) {
    this.dataPointer = builder.dataPointer;
    this.tablePointer = builder.tablePointer;
    this.attributeMappings = builder.attributeMappings;
    this.textSearchMapping = builder.textSearchMapping;
    this.hierarchyMappings = builder.hierarchyMappings;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private String dataPointer;
    private UFTablePointer tablePointer;
    private Map<String, UFAttributeMapping> attributeMappings;
    private UFTextSearchMapping textSearchMapping;
    private Map<String, UFHierarchyMapping> hierarchyMappings;

    public Builder dataPointer(String dataPointer) {
      this.dataPointer = dataPointer;
      return this;
    }

    public Builder tablePointer(UFTablePointer tablePointer) {
      this.tablePointer = tablePointer;
      return this;
    }

    public Builder attributeMappings(Map<String, UFAttributeMapping> attributeMappings) {
      this.attributeMappings = attributeMappings;
      return this;
    }

    public Builder textSearchMapping(UFTextSearchMapping textSearchMapping) {
      this.textSearchMapping = textSearchMapping;
      return this;
    }

    public Builder hierarchyMappings(Map<String, UFHierarchyMapping> hierarchyMappings) {
      this.hierarchyMappings = hierarchyMappings;
      return this;
    }

    /** Call the private constructor. */
    public UFEntityMapping build() {
      return new UFEntityMapping(this);
    }
  }

  public String getDataPointer() {
    return dataPointer;
  }

  public UFTablePointer getTablePointer() {
    return tablePointer;
  }

  public Map<String, UFAttributeMapping> getAttributeMappings() {
    return attributeMappings;
  }

  public UFTextSearchMapping getTextSearchMapping() {
    return textSearchMapping;
  }

  public Map<String, UFHierarchyMapping> getHierarchyMappings() {
    return hierarchyMappings;
  }
}
