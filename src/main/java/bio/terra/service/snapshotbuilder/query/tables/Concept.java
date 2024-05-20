package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;

public class Concept extends Table {

  public static final String TABLE_NAME = "concept";
  public static final String DOMAIN_ID = "domain_id";
  public static final String CONCEPT_NAME = "concept_name";
  public static final String CONCEPT_ID = "concept_id";
  public static final String STANDARD_CONCEPT = "standard_concept";
  public static final String CONCEPT_CODE = "concept_code";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);

  private Concept() {
    super(TableVariable.forPrimary(tablePointer));
  }

  private Concept(String joinField, FieldVariable joinFieldOnParent) {
    super(TableVariable.forJoined(tablePointer, joinField, joinFieldOnParent));
  }

  public static Concept asPrimary() {
    return new Concept();
  }

  public static Concept conceptId(FieldVariable joinFieldOnParent) {
    return new Concept(CONCEPT_ID, joinFieldOnParent);
  }

  public FieldVariable name() {
    return getFieldVariable(CONCEPT_NAME);
  }

  public FieldVariable concept_id() {
    return getFieldVariable(CONCEPT_ID);
  }

  public FieldVariable domain_id() {
    return getFieldVariable(DOMAIN_ID);
  }

  public FieldVariable code() {
    return getFieldVariable(CONCEPT_CODE);
  }

  public FieldVariable standardConcept() {
    return getFieldVariable(STANDARD_CONCEPT);
  }
}
