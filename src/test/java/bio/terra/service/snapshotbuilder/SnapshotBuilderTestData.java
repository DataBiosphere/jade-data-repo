package bio.terra.service.snapshotbuilder;

import bio.terra.common.Column;
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
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;
import bio.terra.service.snapshotbuilder.utils.constants.Observation;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import bio.terra.service.snapshotbuilder.utils.constants.ProcedureOccurrence;
import java.util.List;
import java.util.UUID;

public class SnapshotBuilderTestData {

  private static SnapshotBuilderDomainOption generateSnapshotBuilderDomainOption(
      int id, String tableName, String columnName, String name, SnapshotBuilderConcept root) {
    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption
        .root(root)
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
                      Observation.TABLE_NAME,
                      Observation.OBSERVATION_CONCEPT_ID,
                      "Observation",
                      new SnapshotBuilderConcept()
                          .id(300)
                          .name("Observation")
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
                  new SnapshotBuilderFeatureValueGroup()
                      .name("Condition")
                      .values(List.of("Condition Column 1", "Condition Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .name("Observation")
                      .values(List.of("Observation Column 1", "Observation Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .name("Procedure")
                      .values(List.of("Procedure Column 1", "Procedure Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .name("Surveys")
                      .values(List.of("Surveys Column 1", "Surveys Column 2")),
                  new SnapshotBuilderFeatureValueGroup()
                      .name("Person")
                      .values(List.of("Demographics Column 1", "Demographics Column 2"))))
          .datasetConceptSets(
              List.of(
                  new SnapshotBuilderDatasetConceptSet()
                      .name("Demographics")
                      .featureValueGroupName("Person"),
                  new SnapshotBuilderDatasetConceptSet()
                      .name("All surveys")
                      .featureValueGroupName("Surveys")));

  public static final Dataset DATASET =
      new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.AZURE))
          .tables(
              List.of(
                  new DatasetTable()
                      .name(Person.TABLE_NAME)
                      .columns(
                          List.of(
                              new Column().name(Person.RACE_CONCEPT_ID).type(TableDataType.INTEGER),
                              new Column()
                                  .name(Person.GENDER_CONCEPT_ID)
                                  .type(TableDataType.INTEGER),
                              new Column()
                                  .name(Person.ETHNICITY_CONCEPT_ID)
                                  .type(TableDataType.INTEGER),
                              new Column()
                                  .name(Person.YEAR_OF_BIRTH)
                                  .type(TableDataType.INTEGER)))));

  public static SnapshotBuilderCohort createCohort() {

    return new SnapshotBuilderCohort()
        .name("cohort")
        .addCriteriaGroupsItem(
            new SnapshotBuilderCriteriaGroup()
                .addCriteriaItem(
                    new SnapshotBuilderProgramDataListCriteria()
                        .id(0)
                        .kind(SnapshotBuilderCriteria.KindEnum.LIST))
                .addCriteriaItem(
                    new SnapshotBuilderDomainCriteria()
                        .id(19)
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
        .addValueSetsItem(
            new SnapshotBuilderFeatureValueGroup().name("valueGroup").addValuesItem("value"));
  }

  public static SnapshotAccessRequest createSnapshotAccessRequest(UUID snapshotId) {
    return new SnapshotAccessRequest()
        .id(snapshotId)
        .name("name")
        .researchPurposeStatement("purpose")
        .datasetRequest(createSnapshotBuilderRequest());
  }

  public static SnapshotAccessRequestResponse createSnapshotAccessRequestResponse(UUID snapshotId) {
    SnapshotAccessRequest request = createSnapshotAccessRequest(snapshotId);
    return new SnapshotAccessRequestResponse()
        .id(UUID.randomUUID())
        .datasetId(snapshotId)
        .snapshotName(request.getName())
        .snapshotResearchPurpose(request.getResearchPurposeStatement())
        .snapshotSpecification(request.getDatasetRequest())
        .createdDate("date")
        .createdBy("user@gmail.com")
        .status(SnapshotAccessRequestStatus.SUBMITTED);
  }
}
