package bio.terra.tanagra.serialization;

import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Underlay;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * External representation of an entity.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFEntity.Builder.class)
public class UFEntity {
  private final String name;
  private final String idAttribute;
  private final List<UFAttribute> attributes;
  private final UFEntityMapping sourceDataMapping;
  private final UFEntityMapping indexDataMapping;
  private final @Nullable UFFieldPointer sourceStartDateColumn;

  public UFEntity(Entity entity) {
    this.name = entity.getName();
    this.idAttribute = entity.getIdAttribute().getName();
    this.attributes =
        entity.getAttributes().stream()
            .map(attr -> new UFAttribute(attr))
            .collect(Collectors.toList());
    this.sourceDataMapping = new UFEntityMapping(entity.getMapping(Underlay.MappingType.SOURCE));
    this.indexDataMapping = new UFEntityMapping(entity.getMapping(Underlay.MappingType.INDEX));
    this.sourceStartDateColumn =
        entity.getSourceStartDateColumn() != null
            ? new UFFieldPointer(entity.getSourceStartDateColumn())
            : null;
  }

  private UFEntity(Builder builder) {
    this.name = builder.name;
    this.idAttribute = builder.idAttribute;
    this.attributes = builder.attributes;
    this.sourceDataMapping = builder.sourceDataMapping;
    this.indexDataMapping = builder.indexDataMapping;
    this.sourceStartDateColumn = builder.sourceStartDateColumn;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private String name;
    private String idAttribute;
    private List<UFAttribute> attributes;
    private UFEntityMapping sourceDataMapping;
    private UFEntityMapping indexDataMapping;
    private UFFieldPointer sourceStartDateColumn;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder idAttribute(String idAttribute) {
      this.idAttribute = idAttribute;
      return this;
    }

    public Builder attributes(List<UFAttribute> attributes) {
      this.attributes = attributes;
      return this;
    }

    public Builder sourceDataMapping(UFEntityMapping sourceDataMapping) {
      this.sourceDataMapping = sourceDataMapping;
      return this;
    }

    public Builder indexDataMapping(UFEntityMapping indexDataMapping) {
      this.indexDataMapping = indexDataMapping;
      return this;
    }

    public Builder sourceStartDateColumn(UFFieldPointer sourceStartDateColumn) {
      this.sourceStartDateColumn = sourceStartDateColumn;
      return this;
    }

    /** Call the private constructor. */
    public UFEntity build() {
      return new UFEntity(this);
    }
  }

  public String getName() {
    return name;
  }

  public String getIdAttribute() {
    return idAttribute;
  }

  public List<UFAttribute> getAttributes() {
    return attributes;
  }

  public UFEntityMapping getSourceDataMapping() {
    return sourceDataMapping;
  }

  public UFEntityMapping getIndexDataMapping() {
    return indexDataMapping;
  }

  public UFFieldPointer getSourceStartDateColumn() {
    return sourceStartDateColumn;
  }
}
