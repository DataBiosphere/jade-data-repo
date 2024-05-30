package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.common.PdaoConstant;
import bio.terra.model.SnapshotBuilderProgramDataOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;

public class Person extends Table {

  public static final String TABLE_NAME = "person";
  public static final String PERSON_ID = "person_id";
  public static final String YEAR_OF_BIRTH = "year_of_birth";
  public static final String GENDER_CONCEPT_ID = "gender_concept_id";
  public static final String RACE_CONCEPT_ID = "race_concept_id";
  public static final String ETHNICITY_CONCEPT_ID = "ethnicity_concept_id";
  public static final String ROW_ID = PdaoConstant.PDAO_ROW_ID_COLUMN;
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);
  private final FieldVariable countPerson;

  public Person(SourceVariable sourceVariable) {
    super(sourceVariable);
    this.countPerson = sourceVariable.makeFieldVariable(PERSON_ID, "COUNT", null, true);
  }

  public static Person asPrimary() {
    return new Person(SourceVariable.forPrimary(tablePointer));
  }

  public FieldVariable fromColumn(String columnName) {
    return getFieldVariable(columnName);
  }

  public FieldVariable personId() {
    return getFieldVariable(PERSON_ID);
  }

  public FieldVariable rowId() {
    return getFieldVariable(ROW_ID);
  }

  public FieldVariable variableForOption(SnapshotBuilderProgramDataOption rangeCriteria) {
    return getFieldVariable(rangeCriteria.getColumnName());
  }

  public FieldVariable countPerson() {
    return this.countPerson;
  }
}
