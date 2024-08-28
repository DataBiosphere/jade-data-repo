package bio.terra.service.snapshotbuilder;

import static bio.terra.service.snapshotbuilder.SnapshotBuilderTestData.SNAPSHOT_BUILDER_COHORT_CONDITION_CONCEPT_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class SnapshotAccessRequestModelTest {
  private static final String EXPECTED_LIST_SUMMARY_STRING =
      String.format(
          "The following concepts from Race: %s, %s",
          SnapshotBuilderTestData.RACE_PROGRAM_DATA_LIST_ITEM_ONE.getName(),
          SnapshotBuilderTestData.RACE_PROGRAM_DATA_LIST_ITEM_TWO.getName());
  private static final String EXPECTED_RANGE_SUMMARY_STRING = "Year of birth between 1960 and 1980";
  private static final String EXPECTED_DOMAIN_SUMMARY_STRING = "Condition Concept: name 401";
  public static final int CONCEPT_ID = 401;
  private static final Map<Integer, String> conceptIdsToNames = Map.of(CONCEPT_ID, "name 401");

  @Test
  void toApiResponse() {
    SnapshotAccessRequestModel model = generateSnapshotAccessRequestModel();
    compareModelAndResponseFields(model, model.toApiResponse());
  }

  @Test
  void toApiDetails() {
    SnapshotAccessRequestModel model = generateSnapshotAccessRequestModel();
    String conditionConceptName = "Condition Concept Name";
    String expectedSummaryString =
        "Participants included:\nName: cohort\nGroups:\nMust meet all of:\nThe following concepts from Race: \nCondition Concept: "
            + conditionConceptName
            + "\nYear of birth between 1950 and 2000\nTables included:Drug, Condition\n";
    assertThat(
        model
            .generateModelDetails(
                SnapshotBuilderTestData.SETTINGS,
                Map.of(SNAPSHOT_BUILDER_COHORT_CONDITION_CONCEPT_ID, conditionConceptName))
            .getSummary(),
        equalToCompressingWhiteSpace(expectedSummaryString));
  }

  @Test
  void generateConceptIds() {
    SnapshotAccessRequestModel model = generateSnapshotAccessRequestModel();
    assertThat(model.generateConceptIds(), contains(SNAPSHOT_BUILDER_COHORT_CONDITION_CONCEPT_ID));
  }

  @Test
  void generateSummaryForListCriteria() {
    SnapshotBuilderProgramDataListCriteria listCriteria = generateListCriteria();
    assertThat(
        new SnapshotAccessRequestModel.SummaryGenerator(SnapshotBuilderTestData.SETTINGS, Map.of())
            .generateSummaryForCriteria(listCriteria),
        equalToCompressingWhiteSpace(EXPECTED_LIST_SUMMARY_STRING));
  }

  @Test
  void generateSummaryForRangeCriteria() {
    SnapshotBuilderProgramDataRangeCriteria rangeCriteria = generateRangeCriteria();
    assertThat(
        new SnapshotAccessRequestModel.SummaryGenerator(SnapshotBuilderTestData.SETTINGS, Map.of())
            .generateSummaryForCriteria(rangeCriteria),
        equalToCompressingWhiteSpace(EXPECTED_RANGE_SUMMARY_STRING));
  }

  @Test
  void generateSummaryForDomainCriteria() {
    SnapshotBuilderDomainCriteria domainCriteria = generateDomainCriteria();
    assertThat(
        new SnapshotAccessRequestModel.SummaryGenerator(
                SnapshotBuilderTestData.SETTINGS, conceptIdsToNames)
            .generateSummaryForCriteria(domainCriteria),
        equalToCompressingWhiteSpace(EXPECTED_DOMAIN_SUMMARY_STRING));
  }

  @Test
  void generateSummaryForCriteriaGroup() {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .mustMeet(true)
            .meetAll(true)
            .criteria(
                List.of(generateRangeCriteria(), generateListCriteria(), generateDomainCriteria()));
    assertThat(
        new SnapshotAccessRequestModel.SummaryGenerator(
                SnapshotBuilderTestData.SETTINGS, conceptIdsToNames)
            .generateSummaryForCriteriaGroup(criteriaGroup),
        equalToCompressingWhiteSpace(
            String.format(
                "Must meet all of:%n%s%n%s%n%s",
                EXPECTED_RANGE_SUMMARY_STRING,
                EXPECTED_LIST_SUMMARY_STRING,
                EXPECTED_DOMAIN_SUMMARY_STRING)));
  }

  @Test
  void generateSummaryForCohort() {
    String cohortName = "cohort";
    String expectedCriteriaGroupString = "Must not meet any of:\n";
    SnapshotBuilderCohort cohort =
        new SnapshotBuilderCohort()
            .name(cohortName)
            .criteriaGroups(
                List.of(
                    new SnapshotBuilderCriteriaGroup().mustMeet(false).meetAll(false),
                    new SnapshotBuilderCriteriaGroup().mustMeet(false).meetAll(false)));
    assertThat(
        new SnapshotAccessRequestModel.SummaryGenerator(
                SnapshotBuilderTestData.SETTINGS, conceptIdsToNames)
            .generateSummaryForCohort(cohort),
        equalToCompressingWhiteSpace(
            String.format(
                "Name: %s%nGroups:%n%s%n%s",
                cohortName, expectedCriteriaGroupString, expectedCriteriaGroupString)));
  }

  private SnapshotBuilderProgramDataRangeCriteria generateRangeCriteria() {
    SnapshotBuilderProgramDataRangeCriteria rangeCriteria =
        new SnapshotBuilderProgramDataRangeCriteria();
    rangeCriteria
        .low(1960)
        .high(1980)
        .id(SnapshotBuilderTestData.YEAR_OF_BIRTH_PROGRAM_DATA_ID)
        .kind(SnapshotBuilderCriteria.KindEnum.RANGE);
    return rangeCriteria;
  }

  private SnapshotBuilderProgramDataListCriteria generateListCriteria() {
    SnapshotBuilderProgramDataListCriteria listCriteria =
        new SnapshotBuilderProgramDataListCriteria();
    listCriteria
        .values(
            List.of(
                SnapshotBuilderTestData.RACE_PROGRAM_DATA_LIST_ITEM_ONE.getId(),
                SnapshotBuilderTestData.RACE_PROGRAM_DATA_LIST_ITEM_TWO.getId()))
        .id(SnapshotBuilderTestData.RACE_PROGRAM_DATA_ID)
        .kind(SnapshotBuilderCriteria.KindEnum.LIST);
    return listCriteria;
  }

  private SnapshotBuilderDomainCriteria generateDomainCriteria() {
    SnapshotBuilderDomainCriteria domainCriteria = new SnapshotBuilderDomainCriteria();
    domainCriteria
        .conceptId(CONCEPT_ID)
        .id(SnapshotBuilderTestData.CONDITION_OCCURRENCE_DOMAIN_ID)
        .kind(SnapshotBuilderCriteria.KindEnum.DOMAIN);
    return domainCriteria;
  }

  private SnapshotAccessRequestModel generateSnapshotAccessRequestModel() {
    return new SnapshotAccessRequestModel(
        UUID.randomUUID(),
        "snapshot name",
        "snapshot research purpose",
        UUID.randomUUID(),
        SnapshotBuilderTestData.createSnapshotBuilderRequest(),
        "a@b.com",
        Instant.now(),
        Instant.now(),
        SnapshotAccessRequestStatus.SUBMITTED,
        UUID.randomUUID(),
        "flightid",
        "samGroupName",
        "tdr@serviceaccount.com");
  }

  private void compareModelAndResponseFields(
      SnapshotAccessRequestModel model, SnapshotAccessRequestResponse response) {
    assertThat(model.id(), is(response.getId()));
    assertThat(model.sourceSnapshotId(), is(response.getSourceSnapshotId()));
    assertThat(model.snapshotResearchPurpose(), is(response.getSnapshotResearchPurpose()));
    assertThat(model.snapshotSpecification(), is(response.getSnapshotSpecification()));
    assertThat(model.createdBy(), is(response.getCreatedBy()));
    assertThat(model.createdDate().toString(), is(response.getCreatedDate()));
    assertThat(model.statusUpdatedDate().toString(), is(response.getStatusUpdatedDate()));
    assertThat(model.status(), is(response.getStatus()));
    assertThat(model.flightid(), is(response.getFlightid()));
    assertThat(model.createdSnapshotId(), is(response.getCreatedSnapshotId()));
    assertThat(model.samGroupName(), is(response.getAuthGroupName()));
  }
}
