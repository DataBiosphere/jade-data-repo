package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.serialization.UFRollupInformation;

public class RollupInformation {
  private final FieldPointer id;
  private final FieldPointer count;
  private final FieldPointer displayHints;

  private Relationship relationship;
  private Entity entity;
  private Hierarchy hierarchy;

  public RollupInformation(FieldPointer id, FieldPointer count, FieldPointer displayHints) {
    this.id = id;
    this.count = count;
    this.displayHints = displayHints;
  }

  public void initialize(Relationship relationship, Entity entity, Hierarchy hierarchy) {
    this.relationship = relationship;
    this.entity = entity;
    this.hierarchy = hierarchy;
  }

  public static RollupInformation fromSerialized(
      UFRollupInformation serialized, DataPointer dataPointer) {
    TablePointer table = TablePointer.fromSerialized(serialized.getTable(), dataPointer);
    FieldPointer id = FieldPointer.fromSerialized(serialized.getId(), table);
    FieldPointer count = FieldPointer.fromSerialized(serialized.getCount(), table);
    FieldPointer displayHints = FieldPointer.fromSerialized(serialized.getDisplayHints(), table);
    return new RollupInformation(id, count, displayHints);
  }

  public static RollupInformation defaultIndexMapping(
      Entity rollupEntity, Entity countedEntity, Hierarchy hierarchy) {
    FieldPointer id =
        rollupEntity.getIdAttribute().getMapping(Underlay.MappingType.INDEX).getValue();
    FieldPointer count =
        new FieldPointer.Builder()
            .tablePointer(id.getTablePointer())
            .columnName(
                RelationshipField.getFieldAlias(
                    RelationshipField.Type.COUNT, countedEntity, hierarchy))
            .build();
    FieldPointer displayHints =
        new FieldPointer.Builder()
            .tablePointer(id.getTablePointer())
            .columnName(
                RelationshipField.getFieldAlias(
                    RelationshipField.Type.DISPLAY_HINTS, countedEntity, hierarchy))
            .build();
    return new RollupInformation(id, count, displayHints);
  }

  public TablePointer getTable() {
    return id.getTablePointer();
  }

  public FieldPointer getId() {
    return id;
  }

  public FieldPointer getCount() {
    return count;
  }

  public FieldPointer getDisplayHints() {
    return displayHints;
  }

  public Relationship getRelationship() {
    return relationship;
  }

  public Entity getEntity() {
    return entity;
  }

  public Hierarchy getHierarchy() {
    return hierarchy;
  }
}
