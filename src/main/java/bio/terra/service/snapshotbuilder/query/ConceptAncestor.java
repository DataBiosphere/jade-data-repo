package bio.terra.service.snapshotbuilder.query;

public class ConceptAncestor extends TableVariable {

  public static final String TABLE_NAME = "concept_ancestor";
  public static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  public static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";

  public ConceptAncestor() {
    super(TablePointer.fromTableName(TABLE_NAME), null, null, false);
  }

  public FieldVariable ancestor_concept_id() {
    return this.makeFieldVariable(ANCESTOR_CONCEPT_ID);
  }

  public FieldVariable descendant_concept_id() {
    return this.makeFieldVariable(DESCENDANT_CONCEPT_ID);
  }
}
