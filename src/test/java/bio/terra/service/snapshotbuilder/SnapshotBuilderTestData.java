package bio.terra.service.snapshotbuilder;

import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.model.CloudPlatform;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDatasetConceptSet;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderFeatureValueGroup;
import bio.terra.model.SnapshotBuilderOption;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListItem;
import bio.terra.model.SnapshotBuilderProgramDataListOption;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeOption;
import bio.terra.model.SnapshotBuilderRequest;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestIdModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import bio.terra.service.snapshotbuilder.query.table.Person;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;
import bio.terra.service.snapshotbuilder.utils.constants.DrugExposure;
import bio.terra.service.snapshotbuilder.utils.constants.ObservationOccurrence;
import bio.terra.service.snapshotbuilder.utils.constants.ProcedureOccurrence;
import java.util.List;
import java.util.UUID;

public class SnapshotBuilderTestData {

  private static SnapshotBuilderDomainOption generateSnapshotBuilderDomainOption(
      int id, String tableName, String columnName, String name, SnapshotBuilderConcept root) {
    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption
        .root(root)
        .conceptCount(100)
        .participantCount(100)
        .id(id)
        .tableName(tableName)
        .columnName(columnName)
        .name(name)
        .kind(SnapshotBuilderOption.KindEnum.DOMAIN);
    return domainOption;
  }

  private static SnapshotBuilderProgramDataListOption generateSnapshotBuilderProgramDataListOption(
      int id,
      String tableName,
      String columnName,
      String name,
      List<SnapshotBuilderProgramDataListItem> values) {
    SnapshotBuilderProgramDataListOption listOption = new SnapshotBuilderProgramDataListOption();
    listOption
        .values(values)
        .id(id)
        .tableName(tableName)
        .columnName(columnName)
        .name(name)
        .kind(SnapshotBuilderOption.KindEnum.LIST);
    return listOption;
  }

  private static SnapshotBuilderProgramDataRangeOption
      generateSnapshotBuilderProgramDataRangeOption(
          int id, String tableName, String columnName, String name, Integer min, Integer max) {
    SnapshotBuilderProgramDataRangeOption rangeOption = new SnapshotBuilderProgramDataRangeOption();
    rangeOption
        .min(min)
        .max(max)
        .id(id)
        .tableName(tableName)
        .columnName(columnName)
        .name(name)
        .kind(SnapshotBuilderOption.KindEnum.RANGE);
    return rangeOption;
  }

  public static final int CONDITION_OCCURRENCE_DOMAIN_ID = 10;
  public static final int PROCEDURE_OCCURRENCE_DOMAIN_ID = 11;
  public static final int OBSERVATION_DOMAIN_ID = 12;
  public static final int DRUG_DOMAIN_ID = 13;
  public static final int YEAR_OF_BIRTH_PROGRAM_DATA_ID = 1;
  public static final int ETHNICITY_PROGRAM_DATA_ID = 2;
  public static final int GENDER_PROGRAM_DATA_ID = 3;
  public static final int RACE_PROGRAM_DATA_ID = 4;

  public static final SnapshotBuilderSettings SETTINGS =
      new SnapshotBuilderSettings()
          .domainOptions(
              List.of(
                  generateSnapshotBuilderDomainOption(
                      CONDITION_OCCURRENCE_DOMAIN_ID,
                      ConditionOccurrence.TABLE_NAME,
                      ConditionOccurrence.CONDITION_CONCEPT_ID,
                      "Condition",
                      new SnapshotBuilderConcept()
                          .id(100)
                          .name("Condition")
                          .count(100)
                          .hasChildren(true)),
                  generateSnapshotBuilderDomainOption(
                      PROCEDURE_OCCURRENCE_DOMAIN_ID,
                      ProcedureOccurrence.TABLE_NAME,
                      ProcedureOccurrence.PROCEDURE_CONCEPT_ID,
                      "Procedure",
                      new SnapshotBuilderConcept()
                          .id(200)
                          .name("Procedure")
                          .count(100)
                          .hasChildren(true)),
                  generateSnapshotBuilderDomainOption(
                      OBSERVATION_DOMAIN_ID,
                      ObservationOccurrence.TABLE_NAME,
                      ObservationOccurrence.OBSERVATION_CONCEPT_ID,
                      "Observation",
                      new SnapshotBuilderConcept()
                          .id(300)
                          .name("Observation")
                          .count(100)
                          .hasChildren(true)),
                  // add option for Drug table
                  generateSnapshotBuilderDomainOption(
                      DRUG_DOMAIN_ID,
                      DrugExposure.TABLE_NAME,
                      DrugExposure.DRUG_CONCEPT_ID,
                      "Drug",
                      new SnapshotBuilderConcept()
                          .id(400)
                          .name("Drug")
                          .count(100)
                          .hasChildren(true))))
          .programDataOptions(
              List.of(
                  generateSnapshotBuilderProgramDataRangeOption(
                      YEAR_OF_BIRTH_PROGRAM_DATA_ID,
                      Person.TABLE_NAME,
                      Person.YEAR_OF_BIRTH,
                      "Year of birth",
                      0,
                      100),
                  generateSnapshotBuilderProgramDataListOption(
                      ETHNICITY_PROGRAM_DATA_ID,
                      Person.TABLE_NAME,
                      Person.ETHNICITY_CONCEPT_ID,
                      "Ethnicity",
                      List.of(new SnapshotBuilderProgramDataListItem().id(40).name("unused"))),
                  generateSnapshotBuilderProgramDataListOption(
                      GENDER_PROGRAM_DATA_ID,
                      Person.TABLE_NAME,
                      Person.GENDER_CONCEPT_ID,
                      "Gender Identity",
                      List.of(new SnapshotBuilderProgramDataListItem().id(41).name("unused 2"))),
                  generateSnapshotBuilderProgramDataListOption(
                      RACE_PROGRAM_DATA_ID,
                      Person.TABLE_NAME,
                      Person.RACE_CONCEPT_ID,
                      "Race",
                      List.of(new SnapshotBuilderProgramDataListItem().id(43).name("unused 3")))))
          .featureValueGroups(
              List.of(
                  new SnapshotBuilderFeatureValueGroup().name("Condition"),
                  new SnapshotBuilderFeatureValueGroup().name("Observation"),
                  new SnapshotBuilderFeatureValueGroup().name("Procedure"),
                  new SnapshotBuilderFeatureValueGroup().name("Surveys"),
                  new SnapshotBuilderFeatureValueGroup().name("Person")))
          .datasetConceptSets(
              List.of(
                  new SnapshotBuilderDatasetConceptSet()
                      .name("Demographics")
                      .featureValueGroupName("Person"),
                  new SnapshotBuilderDatasetConceptSet()
                      .name("All surveys")
                      .featureValueGroupName("Surveys")));

  public static final Column PERSON_ID_COLUMN =
      new Column().name("person_id").type(TableDataType.INTEGER);
  public static final Column RACE_CONCEPT_ID_COLUMN =
      new Column().name(Person.RACE_CONCEPT_ID).type(TableDataType.INTEGER);
  public static final Column GENDER_CONCEPT_ID_COLUMN =
      new Column().name(Person.GENDER_CONCEPT_ID).type(TableDataType.INTEGER);
  public static final Column ETHNICITY_CONCEPT_ID_COLUMN =
      new Column().name(Person.ETHNICITY_CONCEPT_ID).type(TableDataType.INTEGER);
  public static final Column YEAR_OF_BIRTH_COLUMN =
      new Column().name(Person.YEAR_OF_BIRTH).type(TableDataType.INTEGER);

  public static final DatasetTable PERSON_TABLE =
      new DatasetTable()
          .name(Person.TABLE_NAME)
          .columns(
              List.of(
                  PERSON_ID_COLUMN,
                  RACE_CONCEPT_ID_COLUMN,
                  GENDER_CONCEPT_ID_COLUMN,
                  ETHNICITY_CONCEPT_ID_COLUMN,
                  YEAR_OF_BIRTH_COLUMN));

  public static final Column CONCEPT_ID_COLUMN =
      new Column().name(Concept.CONCEPT_ID).type(TableDataType.INTEGER);
  public static final Column CONCEPT_NAME_COLUMN =
      new Column().name(Concept.CONCEPT_NAME).type(TableDataType.STRING);
  public static final DatasetTable CONCEPT_TABLE =
      new DatasetTable()
          .name(Concept.TABLE_NAME)
          .columns(List.of(CONCEPT_ID_COLUMN, CONCEPT_NAME_COLUMN));

  private static final Column DRUG_CONCEPT_ID_COLUMN =
      new Column().name(DrugExposure.DRUG_CONCEPT_ID).type(TableDataType.INTEGER);
  private static final Column DRUG_TYPE_CONCEPT_ID_COLUMN =
      new Column().name(DrugExposure.DRUG_TYPE_CONCEPT_ID).type(TableDataType.INTEGER);
  private static final Column DRUG_ROUTE_CONCEPT_ID_COLUMN =
      new Column().name(DrugExposure.DRUG_ROUTE_CONCEPT_ID).type(TableDataType.INTEGER);
  private static final Column DRUG_SOURCE_CONCEPT_ID_COLUMN =
      new Column().name(DrugExposure.DRUG_SOURCE_CONCEPT_ID).type(TableDataType.INTEGER);

  public static final DatasetTable DRUG_TABLE =
      new DatasetTable()
          .name(DrugExposure.TABLE_NAME)
          .columns(
              List.of(
                  PERSON_ID_COLUMN,
                  DRUG_CONCEPT_ID_COLUMN,
                  DRUG_TYPE_CONCEPT_ID_COLUMN,
                  DRUG_ROUTE_CONCEPT_ID_COLUMN,
                  DRUG_SOURCE_CONCEPT_ID_COLUMN));

  private static final Column CONDITION_CONCEPT_ID_COLUMN =
      new Column().name(ConditionOccurrence.CONDITION_CONCEPT_ID).type(TableDataType.INTEGER);
  private static final Column CONDITION_STATUS_CONCEPT_ID_COLUMN =
      new Column()
          .name(ConditionOccurrence.CONDITION_STATUS_CONCEPT_ID)
          .type(TableDataType.INTEGER);
  private static final Column CONDITION_SOURCE_CONCEPT_ID_COLUMN =
      new Column()
          .name(ConditionOccurrence.CONDITION_SOURCE_CONCEPT_ID)
          .type(TableDataType.INTEGER);
  public static final DatasetTable CONDITION_TABLE =
      new DatasetTable()
          .name(ConditionOccurrence.TABLE_NAME)
          .columns(
              List.of(
                  PERSON_ID_COLUMN,
                  CONDITION_CONCEPT_ID_COLUMN,
                  CONDITION_SOURCE_CONCEPT_ID_COLUMN,
                  CONDITION_STATUS_CONCEPT_ID_COLUMN));

  public static final Dataset DATASET =
      new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.AZURE))
          .tables(List.of(PERSON_TABLE, CONCEPT_TABLE, DRUG_TABLE, CONDITION_TABLE))
          .relationships(
              List.of(
                  new Relationship()
                      .name("fpk_drug_person")
                      .id(UUID.randomUUID())
                      .fromTable(DRUG_TABLE)
                      .fromColumn(PERSON_ID_COLUMN)
                      .toTable(PERSON_TABLE)
                      .toColumn(PERSON_ID_COLUMN),
                  new Relationship()
                      .name("fpk_drug_type_concept")
                      .id(UUID.randomUUID())
                      .fromColumn(DRUG_TYPE_CONCEPT_ID_COLUMN)
                      .fromTable(DRUG_TABLE)
                      .toTable(CONCEPT_TABLE)
                      .toColumn(CONCEPT_ID_COLUMN),
                  new Relationship()
                      .name("fpk_drug_concept")
                      .id(UUID.randomUUID())
                      .fromColumn(DRUG_CONCEPT_ID_COLUMN)
                      .fromTable(DRUG_TABLE)
                      .toTable(CONCEPT_TABLE)
                      .toColumn(CONCEPT_ID_COLUMN),
                  new Relationship()
                      .name("fpk_drug_route_concept")
                      .id(UUID.randomUUID())
                      .fromColumn(DRUG_ROUTE_CONCEPT_ID_COLUMN)
                      .fromTable(DRUG_TABLE)
                      .toTable(CONCEPT_TABLE)
                      .toColumn(CONCEPT_ID_COLUMN),
                  new Relationship()
                      .name("fpk_drug_concept_s")
                      .id(UUID.randomUUID())
                      .fromColumn(DRUG_SOURCE_CONCEPT_ID_COLUMN)
                      .fromTable(DRUG_TABLE)
                      .toTable(CONCEPT_TABLE)
                      .toColumn(CONCEPT_ID_COLUMN),
                  new Relationship()
                      .name("fpk_condition_person")
                      .id(UUID.randomUUID())
                      .fromColumn(PERSON_ID_COLUMN)
                      .fromTable(CONDITION_TABLE)
                      .toTable(PERSON_TABLE)
                      .toColumn(PERSON_ID_COLUMN),
                  new Relationship()
                      .name("fpk_condition_concept")
                      .id(UUID.randomUUID())
                      .fromColumn(CONDITION_CONCEPT_ID_COLUMN)
                      .fromTable(CONDITION_TABLE)
                      .toTable(CONCEPT_TABLE)
                      .toColumn(CONCEPT_ID_COLUMN),
                  new Relationship()
                      .name("fpk_condition_status_concept")
                      .id(UUID.randomUUID())
                      .fromColumn(CONDITION_STATUS_CONCEPT_ID_COLUMN)
                      .fromTable(CONDITION_TABLE)
                      .toTable(CONCEPT_TABLE)
                      .toColumn(CONCEPT_ID_COLUMN),
                  new Relationship()
                      .name("fpk_condition_concept_s")
                      .id(UUID.randomUUID())
                      .fromColumn(CONDITION_SOURCE_CONCEPT_ID_COLUMN)
                      .fromTable(CONDITION_TABLE)
                      .toTable(CONCEPT_TABLE)
                      .toColumn(CONCEPT_ID_COLUMN)));

  public static SnapshotBuilderCohort createCohort() {

    return new SnapshotBuilderCohort()
        .name("cohort")
        .addCriteriaGroupsItem(
            new SnapshotBuilderCriteriaGroup()
                .meetAll(true)
                .mustMeet(true)
                .addCriteriaItem(
                    new SnapshotBuilderProgramDataListCriteria()
                        .id(0)
                        .kind(SnapshotBuilderCriteria.KindEnum.LIST))
                .addCriteriaItem(
                    new SnapshotBuilderDomainCriteria()
                        .conceptId(100)
                        .id(CONDITION_OCCURRENCE_DOMAIN_ID)
                        .kind(SnapshotBuilderCriteria.KindEnum.DOMAIN))
                .addCriteriaItem(
                    new SnapshotBuilderProgramDataRangeCriteria()
                        .low(1950)
                        .high(2000)
                        .id(1)
                        .kind(SnapshotBuilderCriteria.KindEnum.RANGE)));
  }

  public static SnapshotBuilderRequest createSnapshotBuilderRequest() {
    return new SnapshotBuilderRequest()
        .addCohortsItem(createCohort())
        .addConceptSetsItem(
            new SnapshotBuilderDatasetConceptSet()
                .name("conceptSet")
                .featureValueGroupName("featureValueGroupName"))
        .addValueSetsItem(new SnapshotBuilderFeatureValueGroup().name("Drug"))
        .addValueSetsItem(new SnapshotBuilderFeatureValueGroup().name("Condition"));
  }

  public static SnapshotAccessRequest createSnapshotAccessRequest(UUID sourceSnapshotId) {
    return new SnapshotAccessRequest()
        .sourceSnapshotId(sourceSnapshotId)
        .name("name")
        .researchPurposeStatement("purpose")
        .snapshotBuilderRequest(createSnapshotBuilderRequest());
  }

  public static SnapshotAccessRequestResponse createSnapshotAccessRequestResponse(UUID snapshotId) {
    SnapshotAccessRequest request = createSnapshotAccessRequest(snapshotId);
    return new SnapshotAccessRequestResponse()
        .id(UUID.randomUUID())
        .sourceSnapshotId(request.getSourceSnapshotId())
        .snapshotName(request.getName())
        .snapshotResearchPurpose(request.getResearchPurposeStatement())
        .snapshotSpecification(request.getSnapshotBuilderRequest())
        .createdDate("date")
        .createdBy("user@gmail.com")
        .status(SnapshotAccessRequestStatus.SUBMITTED);
  }

  public static SnapshotRequestModel createSnapshotRequestByRequestId(
      UUID snapshotAccessRequestId) {
    return new SnapshotRequestModel()
        .name("snapshotRequestName")
        .contents(
            List.of(
                new SnapshotRequestContentsModel()
                    .mode(SnapshotRequestContentsModel.ModeEnum.BYREQUESTID)
                    .requestIdSpec(
                        new SnapshotRequestIdModel().snapshotRequestId(snapshotAccessRequestId))));
  }
}
