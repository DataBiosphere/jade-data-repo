package bio.terra.service.snapshotbuilder.query;

public class Concept extends TableVariable {

  public static final String TABLE_NAME = "concept";
  public static final String DOMAIN_ID = "domain_id";
  public static final String CONCEPT_NAME = "concept_name";
  public static final String CONCEPT_ID = "concept_id";
  public static final String STANDARD_CONCEPT = "standard_concept";
  public static final String CONCEPT_CODE = "concept_code";

  public Concept() {
    super(TablePointer.fromTableName(TABLE_NAME), null, null, false);
  }

  public FieldVariable name() {
    return this.makeFieldVariable(CONCEPT_NAME);
  }

  public FieldVariable id() {
    return this.makeFieldVariable(CONCEPT_ID);
  }

  public FieldVariable code() {
    return this.makeFieldVariable(CONCEPT_CODE);
  }

  public FieldVariable standard_concept() {
    return this.makeFieldVariable(STANDARD_CONCEPT);
  }
}
