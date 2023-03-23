package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.azure.AzureExecutor;
import bio.terra.tanagra.serialization.UFEntity;
import bio.terra.tanagra.serialization.UFHierarchyMapping;
import bio.terra.tanagra.utils.FileIO;
import bio.terra.tanagra.utils.JacksonMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  public static Entity fromJSON(String entityFileName, Map<String, DataPointer> dataPointers)
      throws IOException {
    // Read in entity file.
    Path entityFilePath =
        FileIO.getInputParentDir().resolve(ENTITY_DIRECTORY_NAME).resolve(entityFileName);
    UFEntity serialized =
        JacksonMapper.readFileIntoJavaObject(
            FileIO.getGetFileInputStreamFunction().apply(entityFilePath), UFEntity.class);

    // Attributes.
    if (serialized.getAttributes() == null || serialized.getAttributes().size() == 0) {
      throw new InvalidConfigException("No Attributes defined: " + serialized.getName());
    }
    Map<String, Attribute> attributes = new HashMap<>();
    serialized
        .getAttributes()
        .forEach(as -> attributes.put(as.getName(), Attribute.fromSerialized(as)));

    // ID attribute.
    if (serialized.getIdAttribute() == null || serialized.getIdAttribute().isEmpty()) {
      throw new InvalidConfigException("No id Attribute defined");
    }
    if (!attributes.containsKey(serialized.getIdAttribute())) {
      throw new InvalidConfigException("Id Attribute not found in the set of Attributes");
    }

    // Source+index entity mappings.
    if (serialized.getSourceDataMapping() == null) {
      throw new InvalidConfigException("No source Data Mapping defined");
    }
    EntityMapping sourceDataMapping =
        EntityMapping.fromSerialized(
            serialized.getSourceDataMapping(),
            dataPointers,
            serialized.getName(),
            Underlay.MappingType.SOURCE);
    if (serialized.getIndexDataMapping() == null) {
      throw new InvalidConfigException("No index Data Mapping defined");
    }
    EntityMapping indexDataMapping =
        EntityMapping.fromSerialized(
            serialized.getIndexDataMapping(),
            dataPointers,
            serialized.getName(),
            Underlay.MappingType.INDEX);

    // Source+index attribute mappings.
    deserializeAttributeMappings(serialized, sourceDataMapping, indexDataMapping, attributes);

    // Source+index text search mapping.
    TextSearch textSearch =
        deserializeTextSearch(serialized, sourceDataMapping, indexDataMapping, attributes);

    // Source+index hierarchy mappings.
    Map<String, Hierarchy> hierarchies =
        deserializeHierarchies(
            serialized,
            sourceDataMapping,
            indexDataMapping,
            attributes.get(serialized.getIdAttribute()).getMapping(Underlay.MappingType.INDEX));

    FieldPointer sourceStartDateColumn = null;
    if (serialized.getSourceStartDateColumn() != null) {
      sourceStartDateColumn =
          FieldPointer.fromSerialized(
              serialized.getSourceStartDateColumn(), sourceDataMapping.getTablePointer());
    }

    Entity entity =
        new Entity(
            serialized.getName(),
            serialized.getIdAttribute(),
            attributes,
            hierarchies,
            textSearch,
            sourceDataMapping,
            indexDataMapping,
            sourceStartDateColumn);

    sourceDataMapping.initialize(entity);
    indexDataMapping.initialize(entity);
    if (textSearch != null) {
      textSearch.initialize(entity);
    }
    hierarchies.values().stream().forEach(hierarchy -> hierarchy.initialize(entity));

    return entity;
  }

  private static void deserializeAttributeMappings(
      UFEntity serialized,
      EntityMapping sourceMapping,
      EntityMapping indexMapping,
      Map<String, Attribute> attributes) {
    for (Attribute attribute : attributes.values()) {
      AttributeMapping sourceAttributeMapping =
          AttributeMapping.fromSerialized(
              serialized.getSourceDataMapping().getAttributeMappings().get(attribute.getName()),
              sourceMapping.getTablePointer(),
              attribute);
      AttributeMapping indexAttributeMapping =
          serialized.getIndexDataMapping().getAttributeMappings() != null
              ? AttributeMapping.fromSerialized(
                  serialized.getIndexDataMapping().getAttributeMappings().get(attribute.getName()),
                  indexMapping.getTablePointer(),
                  attribute)
              : AttributeMapping.fromSerialized(null, indexMapping.getTablePointer(), attribute);
      attribute.initialize(sourceAttributeMapping, indexAttributeMapping);
    }
    serialized.getSourceDataMapping().getAttributeMappings().keySet().stream()
        .forEach(
            serializedAttributeName -> {
              if (!attributes.containsKey(serializedAttributeName)) {
                throw new InvalidConfigException(
                    "A source mapping is defined for a non-existent attribute: "
                        + serializedAttributeName);
              }
            });
  }

  private static TextSearch deserializeTextSearch(
      UFEntity serialized,
      EntityMapping sourceMapping,
      EntityMapping indexMapping,
      Map<String, Attribute> attributes) {
    if (serialized.getSourceDataMapping().getTextSearchMapping() == null) {
      return new TextSearch();
    }
    TextSearchMapping sourceTextSearchMapping =
        TextSearchMapping.fromSerialized(
            serialized.getSourceDataMapping().getTextSearchMapping(),
            sourceMapping.getTablePointer(),
            attributes,
            Underlay.MappingType.SOURCE);
    TextSearchMapping indexTextSearchMapping =
        serialized.getIndexDataMapping().getTextSearchMapping() == null
            ? TextSearchMapping.defaultIndexMapping(indexMapping.getTablePointer())
            : TextSearchMapping.fromSerialized(
                serialized.getIndexDataMapping().getTextSearchMapping(),
                indexMapping.getTablePointer(),
                attributes,
                Underlay.MappingType.INDEX);
    return new TextSearch(sourceTextSearchMapping, indexTextSearchMapping);
  }

  private static Map<String, Hierarchy> deserializeHierarchies(
      UFEntity serialized,
      EntityMapping sourceEntityMapping,
      EntityMapping indexEntityMapping,
      AttributeMapping idAttribute) {
    Map<String, UFHierarchyMapping> sourceHierarchyMappingsSerialized =
        serialized.getSourceDataMapping().getHierarchyMappings();
    Map<String, UFHierarchyMapping> indexHierarchyMappingsSerialized =
        serialized.getIndexDataMapping().getHierarchyMappings();
    Map<String, Hierarchy> hierarchies = new HashMap<>();
    if (sourceHierarchyMappingsSerialized != null) {
      sourceHierarchyMappingsSerialized.entrySet().stream()
          .forEach(
              sourceHierarchyMappingSerialized -> {
                HierarchyMapping sourceMapping =
                    HierarchyMapping.fromSerialized(
                        sourceHierarchyMappingSerialized.getValue(),
                        sourceEntityMapping.getTablePointer().getDataPointer(),
                        Underlay.MappingType.SOURCE);
                HierarchyMapping indexMapping =
                    indexHierarchyMappingsSerialized == null
                        ? HierarchyMapping.defaultIndexMapping(
                            serialized.getName(),
                            sourceHierarchyMappingSerialized.getKey(),
                            idAttribute.getValue(),
                            sourceHierarchyMappingSerialized.getValue().getMaxHierarchyDepth())
                        : HierarchyMapping.fromSerialized(
                            indexHierarchyMappingsSerialized.get(
                                sourceHierarchyMappingSerialized.getKey()),
                            indexEntityMapping.getTablePointer().getDataPointer(),
                            Underlay.MappingType.INDEX);
                hierarchies.put(
                    sourceHierarchyMappingSerialized.getKey(),
                    new Hierarchy(
                        sourceHierarchyMappingSerialized.getKey(), sourceMapping, indexMapping));
              });
    }
    return hierarchies;
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

  public List<Attribute> getAttributes() {
    return Collections.unmodifiableList(attributes.values().stream().collect(Collectors.toList()));
  }

  public void addAttribute(String name, Attribute attribute) {
    attributes.put(name, attribute);
  }

  public Hierarchy getHierarchy(String name) {
    return hierarchies.get(name);
  }

  public List<Hierarchy> getHierarchies() {
    return Collections.unmodifiableList(hierarchies.values().stream().collect(Collectors.toList()));
  }

  public boolean hasHierarchies() {
    return hierarchies != null && !hierarchies.isEmpty();
  }

  public TextSearch getTextSearch() {
    return textSearch;
  }

  public List<Relationship> getRelationships() {
    List<Relationship> relationships = new ArrayList<>();
    underlay.getEntityGroups().values().stream()
        .forEach(
            entityGroup ->
                entityGroup.getRelationships().values().stream()
                    .filter(relationship -> relationship.includesEntity(this))
                    .forEach(relationship -> relationships.add(relationship)));
    return relationships;
  }

  public Relationship getRelationship(Entity relatedEntity) {
    return getRelationships().stream()
        .filter(relationship -> relationship.includesEntity(relatedEntity))
        .findFirst()
        .get();
  }

  public Underlay getUnderlay() {
    return underlay;
  }

  public EntityMapping getMapping(Underlay.MappingType mappingType) {
    return Underlay.MappingType.SOURCE.equals(mappingType) ? sourceDataMapping : indexDataMapping;
  }

  public FieldPointer getSourceStartDateColumn() {
    return sourceStartDateColumn;
  }
}
