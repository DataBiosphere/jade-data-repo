package bio.terra.service.snapshotbuilder.utils.constants;

import bio.terra.common.PdaoConstant;

public final class Person {
  private Person() {}

  public static final String TABLE_NAME = "person";
  public static final String PERSON_ID = "person_id";
  public static final String YEAR_OF_BIRTH = "year_of_birth";
  public static final String GENDER_CONCEPT_ID = "gender_concept_id";
  public static final String RACE_CONCEPT_ID = "race_concept_id";
  public static final String ETHNICITY_CONCEPT_ID = "ethnicity_concept_id";
  public static final String ROW_ID = PdaoConstant.PDAO_ROW_ID_COLUMN;
}
