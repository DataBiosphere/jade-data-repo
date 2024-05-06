package bio.terra.service.snapshotbuilder.query;

import bio.terra.service.snapshotbuilder.query.filtervariable.TableVariableBuilder;

public class ConceptRelationship extends TableVariable {

  public static final String TABLE_NAME = "concept_relationship";
  public static final String CONCEPT_ID_1 = "concept_id_1";
  public static final String CONCEPT_ID_2 = "concept_id_2";
  public static final String RELATIONSHIP_ID = "relationship_id";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);

  public ConceptRelationship() {
    super(TablePointer.fromTableName(TABLE_NAME), null, null, false);
  }

  public ConceptRelationship(TableVariableBuilder tableVariableBuilder) {
    super(
        tablePointer,
        tableVariableBuilder.getJoinField(),
        tableVariableBuilder.getJoinFieldOnParent(),
        tableVariableBuilder.isLeftJoin());
  }

  public FieldVariable concept_id_1() {
    return this.makeFieldVariable(CONCEPT_ID_1);
  }

  public FieldVariable concept_id_2() {
    return this.makeFieldVariable(CONCEPT_ID_2);
  }

  public FieldVariable relationship_id() {
    return this.makeFieldVariable(RELATIONSHIP_ID);
  }
}
