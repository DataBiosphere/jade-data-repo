package bio.terra.tanagra.underlay.entitygroup;

import bio.terra.tanagra.serialization.entitygroup.UFCriteriaOccurrence;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.AuxiliaryData;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.EntityGroup;
import bio.terra.tanagra.underlay.EntityGroupMapping;
import bio.terra.tanagra.underlay.Relationship;
import bio.terra.tanagra.underlay.RelationshipField;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.relationshipfield.Count;
import bio.terra.tanagra.underlay.relationshipfield.DisplayHints;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CriteriaOccurrence extends EntityGroup {
  private static final String CRITERIA_ENTITY_NAME = "criteria";
  private static final String OCCURRENCE_ENTITY_NAME = "occurrence";
  private static final String OCCURRENCE_TO_CRITERIA_RELATIONSHIP_NAME = "occurrenceToCriteria";
  private static final String OCCURRENCE_TO_PRIMARY_RELATIONSHIP_NAME = "occurrenceToPrimary";
  private static final String CRITERIA_TO_PRIMARY_RELATIONSHIP_NAME = "criteriaToPrimary";
  // There can be more than one occurrence related entity: occurrenceRelatedEntity0,
  // occurrenceRelatedEntity1, etc
  private static final String OCCURRENCE_RELATED_ENTITY_RELATIONSHIP_NAME_STARTS_WITH =
      "occurrenceRelatedEntity";
  public static final String AGE_AT_OCCURRENCE_ATTRIBUTE_NAME = "age_at_occurrence";

  public static final String MODIFIER_AUX_DATA_ID_COL = "entity_id";
  public static final String MODIFIER_AUX_DATA_ATTR_COL = "attribute_name";
  public static final String MODIFIER_AUX_DATA_MIN_COL = "min";
  public static final String MODIFIER_AUX_DATA_MAX_COL = "max";
  public static final String MODIFIER_AUX_DATA_ENUM_VAL_COL = "enum_value";
  public static final String MODIFIER_AUX_DATA_ENUM_DISPLAY_COL = "enum_display";
  public static final String MODIFIER_AUX_DATA_ENUM_COUNT_COL = "enum_count";

  private static final AuxiliaryData MODIFIER_AUXILIARY_DATA =
      new AuxiliaryData(
          "modifiers",
          List.of(
              MODIFIER_AUX_DATA_ID_COL,
              MODIFIER_AUX_DATA_ATTR_COL,
              MODIFIER_AUX_DATA_MIN_COL,
              MODIFIER_AUX_DATA_MAX_COL,
              MODIFIER_AUX_DATA_ENUM_VAL_COL,
              MODIFIER_AUX_DATA_ENUM_DISPLAY_COL,
              MODIFIER_AUX_DATA_ENUM_COUNT_COL));

  private final Entity criteriaEntity;
  private final Entity occurrenceEntity;
  private final Entity primaryEntity;
  // Entities related to occurrenceEntity
  private final List<Entity> occurrenceRelatedEntities;
  private final List<Attribute> modifierAttributes;
  private final AuxiliaryData modifierAuxiliaryData;

  private CriteriaOccurrence(Builder builder) {
    super(builder);
    this.criteriaEntity = builder.criteriaEntity;
    this.occurrenceEntity = builder.occurrenceEntity;
    this.primaryEntity = builder.primaryEntity;
    this.occurrenceRelatedEntities = builder.occurrenceRelatedEntities;
    this.modifierAttributes = builder.modifierAttributes;
    boolean hasModifierAttributes =
        builder.modifierAttributes != null && !builder.modifierAttributes.isEmpty();
    this.modifierAuxiliaryData =
        hasModifierAttributes ? MODIFIER_AUXILIARY_DATA.cloneWithoutMappings() : null;
  }

  public static CriteriaOccurrence fromSerialized(
      UFCriteriaOccurrence serialized,
      Map<String, DataPointer> dataPointers,
      Map<String, Entity> entities,
      String primaryEntityName) {
    // Entities.
    Entity criteriaEntity = entities.get(serialized.getCriteriaEntity());
    Entity occurrenceEntity = entities.get(serialized.getOccurrenceEntity());
    Entity primaryEntity = entities.get(primaryEntityName);
    List<Entity> occurrenceRelatedEntities =
        serialized.getOccurrenceRelatedEntities() != null
            ? serialized.getOccurrenceRelatedEntities().stream()
                .map(e -> entities.get(e))
                .collect(Collectors.toList())
            : List.of();

    // Modifier attributes.
    List<Attribute> modifierAttributes =
        serialized.getModifierAttributes() == null
            ? Collections.emptyList()
            : serialized.getModifierAttributes().stream()
                .map(attrName -> occurrenceEntity.getAttribute(attrName))
                .collect(Collectors.toList());

    // Relationships.
    Map<String, Relationship> relationships =
        new HashMap<>(
            Map.of(
                OCCURRENCE_TO_CRITERIA_RELATIONSHIP_NAME,
                new Relationship(
                    OCCURRENCE_TO_CRITERIA_RELATIONSHIP_NAME,
                    occurrenceEntity,
                    criteriaEntity,
                    buildRelationshipFieldList(criteriaEntity)),
                OCCURRENCE_TO_PRIMARY_RELATIONSHIP_NAME,
                new Relationship(
                    OCCURRENCE_TO_PRIMARY_RELATIONSHIP_NAME,
                    occurrenceEntity,
                    primaryEntity,
                    Collections.emptyList()),
                CRITERIA_TO_PRIMARY_RELATIONSHIP_NAME,
                new Relationship(
                    CRITERIA_TO_PRIMARY_RELATIONSHIP_NAME,
                    criteriaEntity,
                    primaryEntity,
                    buildRelationshipFieldList(criteriaEntity))));
    for (int i = 0; i < occurrenceRelatedEntities.size(); i++) {
      String relationshipName = OCCURRENCE_RELATED_ENTITY_RELATIONSHIP_NAME_STARTS_WITH + i;
      relationships.put(
          relationshipName,
          new Relationship(
              relationshipName,
              occurrenceEntity,
              occurrenceRelatedEntities.get(i),
              buildRelationshipFieldList(criteriaEntity)));
    }

    // Source+index entity group mappings.
    EntityGroupMapping sourceDataMapping =
        EntityGroupMapping.fromSerialized(
            serialized.getSourceDataMapping(), dataPointers, Underlay.MappingType.SOURCE);
    EntityGroupMapping indexDataMapping =
        EntityGroupMapping.fromSerialized(
            serialized.getIndexDataMapping(), dataPointers, Underlay.MappingType.INDEX);

    Builder builder = new Builder();
    builder
        .name(serialized.getName())
        .relationships(relationships)
        .sourceDataMapping(sourceDataMapping)
        .indexDataMapping(indexDataMapping);
    CriteriaOccurrence criteriaOccurrence =
        builder
            .criteriaEntity(criteriaEntity)
            .occurrenceEntity(occurrenceEntity)
            .primaryEntity(primaryEntity)
            .occurrenceRelatedEntities(occurrenceRelatedEntities)
            .modifierAttributes(modifierAttributes)
            .build();

    sourceDataMapping.initialize(criteriaOccurrence);
    indexDataMapping.initialize(criteriaOccurrence);

    // Source+index relationship mappings.
    EntityGroup.deserializeRelationshipMappings(serialized, criteriaOccurrence);

    // Source+index auxiliary data mappings.
    EntityGroup.deserializeAuxiliaryDataMappings(serialized, criteriaOccurrence);

    return criteriaOccurrence;
  }

  private static List<RelationshipField> buildRelationshipFieldList(Entity entity) {
    List<RelationshipField> fields = new ArrayList<>();
    fields.add(new Count(entity));
    fields.add(new DisplayHints(entity));

    if (entity.hasHierarchies()) {
      entity.getHierarchies().stream()
          .forEach(
              hierarchy -> {
                fields.add(new Count(entity, hierarchy));
                fields.add(new DisplayHints(entity, hierarchy));
              });
    }
    return fields;
  }

  @Override
  public Type getType() {
    return Type.CRITERIA_OCCURRENCE;
  }

  @Override
  public Map<String, Entity> getEntityMap() {
    return ImmutableMap.of(
        CRITERIA_ENTITY_NAME, criteriaEntity, OCCURRENCE_ENTITY_NAME, occurrenceEntity);
  }

  public Entity getCriteriaEntity() {
    return criteriaEntity;
  }

  public Entity getPrimaryEntity() {
    return primaryEntity;
  }

  public Entity getOccurrenceEntity() {
    return occurrenceEntity;
  }

  public List<Entity> getOccurrenceRelatedEntities() {
    return occurrenceRelatedEntities;
  }

  public List<Attribute> getModifierAttributes() {
    return Collections.unmodifiableList(modifierAttributes);
  }

  public AuxiliaryData getModifierAuxiliaryData() {
    return modifierAuxiliaryData;
  }

  @Override
  public List<AuxiliaryData> getAuxiliaryData() {
    return modifierAuxiliaryData == null ? Collections.emptyList() : List.of(modifierAuxiliaryData);
  }

  @Override
  public UFCriteriaOccurrence serialize() {
    return new UFCriteriaOccurrence(this);
  }

  public Relationship getCriteriaPrimaryRelationship() {
    return relationships.get(CRITERIA_TO_PRIMARY_RELATIONSHIP_NAME);
  }

  public Relationship getOccurrenceCriteriaRelationship() {
    return relationships.get(OCCURRENCE_TO_CRITERIA_RELATIONSHIP_NAME);
  }

  public Relationship getOccurrencePrimaryRelationship() {
    return relationships.get(OCCURRENCE_TO_PRIMARY_RELATIONSHIP_NAME);
  }

  private static class Builder extends EntityGroup.Builder {
    private Entity criteriaEntity;
    private Entity occurrenceEntity;
    private Entity primaryEntity;
    private List<Entity> occurrenceRelatedEntities;
    private List<Attribute> modifierAttributes;

    public Builder criteriaEntity(Entity criteriaEntity) {
      this.criteriaEntity = criteriaEntity;
      return this;
    }

    public Builder occurrenceEntity(Entity occurrenceEntity) {
      this.occurrenceEntity = occurrenceEntity;
      return this;
    }

    public Builder primaryEntity(Entity primaryEntity) {
      this.primaryEntity = primaryEntity;
      return this;
    }

    public Builder occurrenceRelatedEntities(List<Entity> occurrenceRelatedEntities) {
      this.occurrenceRelatedEntities = occurrenceRelatedEntities;
      return this;
    }

    public Builder modifierAttributes(List<Attribute> modifierAttributes) {
      this.modifierAttributes = modifierAttributes;
      return this;
    }

    @Override
    public CriteriaOccurrence build() {
      return new CriteriaOccurrence(this);
    }
  }
}
