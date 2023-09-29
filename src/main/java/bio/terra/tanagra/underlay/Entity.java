package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.azure.AzureExecutor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public final class Entity {
  public static final String ENTITY_DIRECTORY_NAME = "entity";

  private final String name;
  private final String idAttributeName;
  private final Map<String, Attribute> attributes;
  private final Map<String, Hierarchy> hierarchies;
  private final TextSearch textSearch;
  private final EntityMapping sourceDataMapping;
  private final EntityMapping indexDataMapping;
  private Underlay underlay;

  // Used to compute age_at_occurrence column on occurrence tables.
  private final @Nullable FieldPointer sourceStartDateColumn;

  @SuppressWarnings("checkstyle:ParameterNumber")
  private Entity(
      String name,
      String idAttributeName,
      Map<String, Attribute> attributes,
      Map<String, Hierarchy> hierarchies,
      TextSearch textSearch,
      EntityMapping sourceDataMapping,
      EntityMapping indexDataMapping,
      @Nullable FieldPointer sourceStartDateColumn) {
    this.name = name;
    this.idAttributeName = idAttributeName;
    this.attributes = attributes;
    this.hierarchies = hierarchies;
    this.textSearch = textSearch;
    this.sourceDataMapping = sourceDataMapping;
    this.indexDataMapping = indexDataMapping;
    this.sourceStartDateColumn = sourceStartDateColumn;
  }

  public void initialize(Underlay underlay) {
    this.underlay = underlay;
  }

  public void scanSourceData(AzureExecutor executor) {
    // lookup the data type and calculate a display hint for each attribute
    for (Attribute attribute : attributes.values()) {
      AttributeMapping attributeMapping = attribute.getMapping(Underlay.MappingType.SOURCE);

      // lookup the datatype
      attribute.setDataType(attributeMapping.computeDataType(executor));

      // generate the display hint
      if (!isIdAttribute(attribute)) {
        attribute.setDisplayHint(attributeMapping.computeDisplayHint(executor));
      }
    }
  }

  public String getName() {
    return name;
  }

  public Attribute getIdAttribute() {
    return attributes.get(idAttributeName);
  }

  public boolean isIdAttribute(Attribute attribute) {
    return attribute.getName().equals(idAttributeName);
  }

  public Attribute getAttribute(String name) {
    return attributes.get(name);
  }

  public Collection<Attribute> getAttributes() {
    return Collections.unmodifiableCollection(attributes.values());
  }

  public void addAttribute(String name, Attribute attribute) {
    attributes.put(name, attribute);
  }

  public Hierarchy getHierarchy(String name) {
    return hierarchies.get(name);
  }

  public Collection<Hierarchy> getHierarchies() {
    return Collections.unmodifiableCollection(hierarchies.values());
  }

  public boolean hasHierarchies() {
    return hierarchies != null && !hierarchies.isEmpty();
  }

  public TextSearch getTextSearch() {
    return textSearch;
  }

  public List<Relationship> getRelationships() {
    List<Relationship> relationships = new ArrayList<>();
    for (EntityGroup entityGroup : underlay.getEntityGroups().values()) {
      entityGroup.getRelationships().values().stream()
          .filter(relationship -> relationship.includesEntity(this))
          .forEach(relationships::add);
    }
    return relationships;
  }

  public Relationship getRelationship(Entity relatedEntity) {
    return getRelationships().stream()
        .filter(relationship -> relationship.includesEntity(relatedEntity))
        .findFirst()
        .orElseThrow();
  }

  public Underlay getUnderlay() {
    return underlay;
  }

  public EntityMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE == mappingType ? sourceDataMapping : indexDataMapping;
  }

  public @Nullable FieldPointer getSourceStartDateColumn() {
    return sourceStartDateColumn;
  }
}
