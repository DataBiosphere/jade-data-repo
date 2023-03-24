package bio.terra.tanagra.underlay;

import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.serialization.UFRelationshipMapping;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RelationshipMapping {
  public static final String COUNT_FIELD_PREFIX = "count_";
  public static final String DISPLAY_HINTS_FIELD_PREFIX = "displayhints_";
  private static final String ID_PAIRS_TABLE_PREFIX = "idpairs_";
  private static final String ID_FIELD_NAME_PREFIX = "id_";
  public static final String NO_HIERARCHY_KEY = "NO_HIERARCHY";

  private final FieldPointer idPairsIdA;
  private final FieldPointer idPairsIdB;
  private final Map<String, RollupInformation> rollupInformationMapA;
  private final Map<String, RollupInformation> rollupInformationMapB;

  private Relationship relationship;

  private RelationshipMapping(
      FieldPointer idPairsIdA,
      FieldPointer idPairsIdB,
      Map<String, RollupInformation> rollupInformationMapA,
      Map<String, RollupInformation> rollupInformationMapB) {
    this.idPairsIdA = idPairsIdA;
    this.idPairsIdB = idPairsIdB;
    this.rollupInformationMapA = rollupInformationMapA;
    this.rollupInformationMapB = rollupInformationMapB;
  }

  public void initialize(Relationship relationship) {
    this.relationship = relationship;
  }

  public static RelationshipMapping fromSerialized(
      UFRelationshipMapping serialized, DataPointer dataPointer) {
    // ID pairs table.
    TablePointer idPairsTable =
        TablePointer.fromSerialized(serialized.getIdPairsTable(), dataPointer);
    FieldPointer idPairsIdA = FieldPointer.fromSerialized(serialized.getIdPairsIdA(), idPairsTable);
    FieldPointer idPairsIdB = FieldPointer.fromSerialized(serialized.getIdPairsIdB(), idPairsTable);

    // Rollup columns for entity A.
    Map<String, RollupInformation> rollupInformationMapA = new HashMap<>();
    if (serialized.getRollupInformationMapA() != null) {
      serialized.getRollupInformationMapA().entrySet().stream()
          .forEach(
              entry ->
                  rollupInformationMapA.put(
                      entry.getKey(),
                      RollupInformation.fromSerialized(entry.getValue(), dataPointer)));
    }

    // Rollup columns for entity B.
    Map<String, RollupInformation> rollupInformationMapB = new HashMap<>();
    if (serialized.getRollupInformationMapB() != null) {
      serialized.getRollupInformationMapB().entrySet().stream()
          .forEach(
              entry ->
                  rollupInformationMapB.put(
                      entry.getKey(),
                      RollupInformation.fromSerialized(entry.getValue(), dataPointer)));
    }

    return new RelationshipMapping(
        idPairsIdA, idPairsIdB, rollupInformationMapA, rollupInformationMapB);
  }

  public static RelationshipMapping defaultIndexMapping(
      DataPointer dataPointer, Relationship relationship) {
    // ID pairs table.
    TablePointer idPairsTable =
        TablePointer.fromTableName(
            ID_PAIRS_TABLE_PREFIX
                + relationship.getEntityA().getName()
                + "_"
                + relationship.getEntityB().getName(),
            dataPointer);
    FieldPointer idPairsIdA =
        new FieldPointer.Builder()
            .tablePointer(idPairsTable)
            .columnName(ID_FIELD_NAME_PREFIX + relationship.getEntityA().getName())
            .build();
    FieldPointer idPairsIdB =
        new FieldPointer.Builder()
            .tablePointer(idPairsTable)
            .columnName(ID_FIELD_NAME_PREFIX + relationship.getEntityB().getName())
            .build();

    // Rollup columns in entity A table.
    Map<String, RollupInformation> rollupInformationMapA = new HashMap<>();
    rollupInformationMapA.put(
        NO_HIERARCHY_KEY,
        RollupInformation.defaultIndexMapping(
            relationship.getEntityA(), relationship.getEntityB(), null));
    if (relationship.getEntityA().hasHierarchies()) {
      relationship.getEntityA().getHierarchies().stream()
          .forEach(
              hierarchy ->
                  rollupInformationMapA.put(
                      hierarchy.getName(),
                      RollupInformation.defaultIndexMapping(
                          relationship.getEntityA(), relationship.getEntityB(), hierarchy)));
    }

    // Rollup columns in entity B table.
    Map<String, RollupInformation> rollupInformationMapB = new HashMap<>();
    rollupInformationMapB.put(
        NO_HIERARCHY_KEY,
        RollupInformation.defaultIndexMapping(
            relationship.getEntityB(), relationship.getEntityA(), null));
    if (relationship.getEntityB().hasHierarchies()) {
      relationship.getEntityB().getHierarchies().stream()
          .forEach(
              hierarchy ->
                  rollupInformationMapB.put(
                      hierarchy.getName(),
                      RollupInformation.defaultIndexMapping(
                          relationship.getEntityB(), relationship.getEntityA(), hierarchy)));
    }

    return new RelationshipMapping(
        idPairsIdA, idPairsIdB, rollupInformationMapA, rollupInformationMapB);
  }

  public Query queryIdPairs(String idAAlias, String idBAlias) {
    TableVariable tableVariable = TableVariable.forPrimary(getIdPairsTable());
    FieldVariable idAFieldVar = new FieldVariable(getIdPairsIdA(), tableVariable, idAAlias);
    FieldVariable idBFieldVar = new FieldVariable(getIdPairsIdB(), tableVariable, idBAlias);
    return new Query.Builder()
        .select(List.of(idAFieldVar, idBFieldVar))
        .tables(List.of(tableVariable))
        .build();
  }

  public FieldPointer getIdPairsId(Entity entity) {
    return (relationship.getEntityA().equals(entity)) ? getIdPairsIdA() : getIdPairsIdB();
  }

  public TablePointer getIdPairsTable() {
    return idPairsIdA.getTablePointer();
  }

  public FieldPointer getIdPairsIdA() {
    return idPairsIdA;
  }

  public FieldPointer getIdPairsIdB() {
    return idPairsIdB;
  }

  public RollupInformation getRollupInfo(Entity entity, Hierarchy hierarchy) {
    return relationship.getEntityA().equals(entity)
        ? getRollupInfoA(hierarchy)
        : getRollupInfoB(hierarchy);
  }

  public RollupInformation getRollupInfoA(Hierarchy hierarchy) {
    return rollupInformationMapA.get(hierarchy == null ? NO_HIERARCHY_KEY : hierarchy.getName());
  }

  public RollupInformation getRollupInfoB(Hierarchy hierarchy) {
    return rollupInformationMapB.get(hierarchy == null ? NO_HIERARCHY_KEY : hierarchy.getName());
  }

  public Map<String, RollupInformation> getRollupInformationMapA() {
    return Collections.unmodifiableMap(rollupInformationMapA);
  }

  public Map<String, RollupInformation> getRollupInformationMapB() {
    return Collections.unmodifiableMap(rollupInformationMapB);
  }
}
