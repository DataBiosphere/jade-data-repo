package bio.terra.service.snapshotbuilder.query;

import bio.terra.service.snapshotbuilder.query.filtervariable.TableVariableBuilder;

public class ConceptAncestor extends TableVariable {

  public static final String TABLE_NAME = "concept_ancestor";
  public static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  public static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);

  public ConceptAncestor() {
    super(tablePointer, null, null, false);
  }

  public ConceptAncestor(TableVariableBuilder tableVariableBuilder) {
    super(
        tablePointer,
        tableVariableBuilder.getJoinField(),
        tableVariableBuilder.getJoinFieldOnParent(),
        tableVariableBuilder.isLeftJoin());
  }

  public FieldVariable ancestor_concept_id() {
    return this.makeFieldVariable(ANCESTOR_CONCEPT_ID);
  }

  public FieldVariable descendant_concept_id() {
    return this.makeFieldVariable(DESCENDANT_CONCEPT_ID);
  }
}
