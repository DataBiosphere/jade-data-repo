package bio.terra.service.snapshotbuilder.query;

public class ConceptRelationship extends TableVariable {

  public static final String TABLE_NAME = "concept_relationship";
  public static final String CONCEPT_ID_1 = "concept_id_1";
  public static final String CONCEPT_ID_2 = "concept_id_2";
  public static final String RELATIONSHIP_ID = "relationship_id";

  public ConceptRelationship() {
    super(TablePointer.fromTableName(TABLE_NAME), null, null, false);
  }

  public FieldVariable concept_id_1() {
    return this.makeFieldVariable(CONCEPT_ID_1);
  }

  public FieldVariable concept_id_2() {
    return this.makeFieldVariable(CONCEPT_ID_2);
  }

  public FieldVariable relationship_id() {
    return this.makeFieldVariable(CONCEPT_ID_2);
  }
}
