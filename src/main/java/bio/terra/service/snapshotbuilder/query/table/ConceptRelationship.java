package bio.terra.service.snapshotbuilder.query.table;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;

public class ConceptRelationship extends Table {

  public static final String TABLE_NAME = "concept_relationship";
  public static final String CONCEPT_ID_1 = "concept_id_1";
  public static final String CONCEPT_ID_2 = "concept_id_2";
  public static final String RELATIONSHIP_ID = "relationship_id";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);

  private ConceptRelationship() {
    super(SourceVariable.forPrimary(tablePointer));
  }

  public static ConceptRelationship asPrimary() {
    return new ConceptRelationship();
  }

  public FieldVariable conceptId1() {
    return getFieldVariable(CONCEPT_ID_1);
  }

  public FieldVariable conceptId2() {
    return getFieldVariable(CONCEPT_ID_2);
  }

  public FieldVariable relationshipId() {
    return getFieldVariable(RELATIONSHIP_ID);
  }
}
