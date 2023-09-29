package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.Literal;

public final class Attribute {

  /** Enum for the types of attributes supported by Tanagra. */
  public enum Type {
    SIMPLE,
    KEY_AND_DISPLAY
  }

  private final String name;
  private final Type type;
  private Literal.DataType dataType;
  private DisplayHint displayHint;
  private AttributeMapping sourceMapping;
  private AttributeMapping indexMapping;

  public Attribute(String name, Type type, Literal.DataType dataType, DisplayHint displayHint) {
    this.name = name;
    this.type = type;
    this.dataType = dataType;
    this.displayHint = displayHint;
  }

  public void initialize(AttributeMapping sourceMapping, AttributeMapping indexMapping) {
    this.sourceMapping = sourceMapping;
    this.indexMapping = indexMapping;
    // sourceMapping is null for age_of_occurrence attribute
    if (sourceMapping != null) {
      sourceMapping.initialize(this);
    }
    indexMapping.initialize(this);
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public Literal.DataType getDataType() {
    return dataType;
  }

  public void setDataType(Literal.DataType dataType) {
    this.dataType = dataType;
  }

  public DisplayHint getDisplayHint() {
    return displayHint;
  }

  public void setDisplayHint(DisplayHint displayHint) {
    this.displayHint = displayHint;
  }

  public AttributeMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE == mappingType ? sourceMapping : indexMapping;
  }
}
