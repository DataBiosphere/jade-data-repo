package bio.terra.service.snapshotbuilder;

import bio.terra.common.Column;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
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
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataOption;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderRequest;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import java.util.List;
import java.util.UUID;

public class SnapshotBuilderTestData {
  public static final SnapshotBuilderSettings SETTINGS =
      new SnapshotBuilderSettings()
          .domainOptions(
              List.of(
                  new SnapshotBuilderDomainOption()
                      .id(10)
                      .category("Condition")
                      .root(
                          new SnapshotBuilderConcept()
                              .id(100)
                              .name("Condition")
                              .count(100)
                              .hasChildren(true)),
                  new SnapshotBuilderDomainOption()
                      .id(11)
                      .category("Procedure")
                      .root(
                          new SnapshotBuilderConcept()
                              .id(200)
                              .name("Procedure")
                              .count(100)
                              .hasChildren(true)),
                  new SnapshotBuilderDomainOption()
                      .id(12)
                      .category("Observation")
                      .root(
                          new SnapshotBuilderConcept()
                              .id(300)
                              .name("Observation")
                              .count(100)
                              .hasChildren(true))))
          .programDataOptions(
              List.of(
                  new SnapshotBuilderProgramDataOption()
                      .id(1)
                      .name("Year of birth")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.RANGE)
                      .tableName("person")
                      .columnName("year_of_birth"),
                  new SnapshotBuilderProgramDataOption()
                      .id(2)
                      .name("Ethnicity")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                      .tableName("person")
                      .columnName("ethnicity"),
                  new SnapshotBuilderProgramDataOption()
                      .id(3)
                      .name("Gender identity")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                      .tableName("person")
                      .columnName("gender_identity"),
                  new SnapshotBuilderProgramDataOption()
                      .id(4)
                      .name("Race")
                      .kind(SnapshotBuilderProgramDataOption.KindEnum.LIST)
                      .tableName("person")
                      .columnName("race")))
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
                      .name("person")
                      .columns(
                          List.of(
                              new Column().name("race").type(TableDataType.INTEGER),
                              new Column().name("gender_identity").type(TableDataType.INTEGER),
                              new Column().name("ethnicity").type(TableDataType.INTEGER),
                              new Column().name("year_of_birth").type(TableDataType.INTEGER)))));

  public static SnapshotBuilderCohort createCohort() {
    return new SnapshotBuilderCohort()
        .name("cohort")
        .addCriteriaGroupsItem(
            new SnapshotBuilderCriteriaGroup()
                .addCriteriaItem(
                    new SnapshotBuilderProgramDataListCriteria()
                        .kind(SnapshotBuilderCriteria.KindEnum.LIST))
                .addCriteriaItem(
                    new SnapshotBuilderDomainCriteria()
                        .domain("condition")
                        .kind(SnapshotBuilderCriteria.KindEnum.DOMAIN))
                .addCriteriaItem(
                    new SnapshotBuilderProgramDataRangeCriteria()
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

  public static SnapshotAccessRequest createSnapshotAccessRequest() {
    return new SnapshotAccessRequest()
        .name("name")
        .researchPurposeStatement("purpose")
        .datasetRequest(createSnapshotBuilderRequest());
  }

  public static SnapshotAccessRequestResponse createSnapshotAccessRequestResponse() {
    return new SnapshotAccessRequestResponse()
        .id(UUID.randomUUID())
        .datasetId(UUID.randomUUID())
        .snapshotName(createSnapshotAccessRequest().getName())
        .snapshotResearchPurpose(createSnapshotAccessRequest().getResearchPurposeStatement())
        .snapshotSpecification(createSnapshotAccessRequest().getDatasetRequest())
        .createdDate("date")
        .createdBy("user@gmail.com")
        .status(SnapshotAccessRequestStatus.SUBMITTED);
  }

  public static EnumerateSnapshotAccessRequestItem createEnumerateSnapshotAccessRequestModelItem() {
    return new EnumerateSnapshotAccessRequestItem()
        .id(UUID.randomUUID())
        .name(createSnapshotAccessRequest().getName())
        .researchPurpose(createSnapshotAccessRequest().getResearchPurposeStatement())
        .createdDate(createSnapshotAccessRequestResponse().getCreatedDate())
        .status(SnapshotAccessRequestStatus.SUBMITTED);
  }
}
