package bio.terra.tanagra.serialization;

import bio.terra.tanagra.serialization.entitygroup.UFCriteriaOccurrence;
import bio.terra.tanagra.serialization.entitygroup.UFGroupItems;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.EntityGroup;
import bio.terra.tanagra.underlay.Underlay;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Map;

/**
 * External representation of an entity group.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = UFCriteriaOccurrence.class, name = "CRITERIA_OCCURRENCE"),
  @JsonSubTypes.Type(value = UFGroupItems.class, name = "GROUP_ITEMS")
})
@JsonDeserialize(builder = UFEntityGroup.Builder.class)
public abstract class UFEntityGroup {
  private final EntityGroup.Type type;
  private final String name;
  private final UFEntityGroupMapping sourceDataMapping;
  private final UFEntityGroupMapping indexDataMapping;

  public UFEntityGroup(EntityGroup entityGroup) {
    this.type = entityGroup.getType();
    this.name = entityGroup.getName();
    this.sourceDataMapping =
        new UFEntityGroupMapping(entityGroup.getMapping(Underlay.MappingType.SOURCE));
    this.indexDataMapping =
        new UFEntityGroupMapping(entityGroup.getMapping(Underlay.MappingType.INDEX));
  }

  protected UFEntityGroup(Builder builder) {
    this.type = builder.type;
    this.name = builder.name;
    this.sourceDataMapping = builder.sourceDataMapping;
    this.indexDataMapping = builder.indexDataMapping;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public abstract static class Builder {
    private EntityGroup.Type type;
    private String name;
    private UFEntityGroupMapping sourceDataMapping;
    private UFEntityGroupMapping indexDataMapping;

    public Builder type(EntityGroup.Type type) {
      this.type = type;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder sourceDataMapping(UFEntityGroupMapping sourceDataMapping) {
      this.sourceDataMapping = sourceDataMapping;
      return this;
    }

    public Builder indexDataMapping(UFEntityGroupMapping indexDataMapping) {
      this.indexDataMapping = indexDataMapping;
      return this;
    }

    public abstract UFEntityGroup build();
  }

  public abstract EntityGroup deserializeToInternal(
      Map<String, DataPointer> dataPointers,
      Map<String, Entity> entities,
      String primaryEntityName);

  public EntityGroup.Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public UFEntityGroupMapping getSourceDataMapping() {
    return sourceDataMapping;
  }

  public UFEntityGroupMapping getIndexDataMapping() {
    return indexDataMapping;
  }
}
