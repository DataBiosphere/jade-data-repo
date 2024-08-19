package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
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
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class SnapshotAccessRequestModelTest {
  private static final String EXPECTED_LIST_SUMMARY_STRING =
      "The following concepts from Race: 0, 1, 2";
  private static final String EXPECTED_RANGE_SUMMARY_STRING = "Year of birth between 1960 and 1980";
  private static final String EXPECTED_DOMAIN_SUMMARY_STRING = "Condition Concept Id: 401";

  @Test
  void toApiResponse() {
    SnapshotAccessRequestModel model = generateSnapshotAccessRequestModel();
    compareModelAndResponseFields(model, model.toApiResponse(SnapshotBuilderTestData.SETTINGS));
  }

  @Test
  void generateSummaryForListCriteria() {
    SnapshotBuilderProgramDataListCriteria listCriteria = generateListCriteria();
    assertThat(
        SnapshotAccessRequestModel.generateSummaryForCriteria(
            listCriteria, SnapshotBuilderTestData.SETTINGS),
        equalToCompressingWhiteSpace(EXPECTED_LIST_SUMMARY_STRING));
  }

  @Test
  void generateSummaryForRangeCriteria() {
    SnapshotBuilderProgramDataRangeCriteria rangeCriteria = generateRangeCriteria();
    assertThat(
        SnapshotAccessRequestModel.generateSummaryForCriteria(
            rangeCriteria, SnapshotBuilderTestData.SETTINGS),
        equalToCompressingWhiteSpace(EXPECTED_RANGE_SUMMARY_STRING));
  }

  @Test
  void generateSummaryForDomainCriteria() {
    SnapshotBuilderDomainCriteria domainCriteria = generateDomainCriteria();
    assertThat(
        SnapshotAccessRequestModel.generateSummaryForCriteria(
            domainCriteria, SnapshotBuilderTestData.SETTINGS),
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
        SnapshotAccessRequestModel.generateSummaryForCriteriaGroup(
            criteriaGroup, SnapshotBuilderTestData.SETTINGS),
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
        SnapshotAccessRequestModel.generateSummaryForCohort(
            cohort, SnapshotBuilderTestData.SETTINGS),
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
        .values(List.of(0, 1, 2))
        .id(SnapshotBuilderTestData.RACE_PROGRAM_DATA_ID)
        .kind(SnapshotBuilderCriteria.KindEnum.LIST);
    return listCriteria;
  }

  private SnapshotBuilderDomainCriteria generateDomainCriteria() {
    SnapshotBuilderDomainCriteria domainCriteria = new SnapshotBuilderDomainCriteria();
    domainCriteria
        .conceptId(401)
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
        "samGroupName@firecloud.org",
        "tdr@serviceaccount.com");
  }

  private void compareModelAndResponseFields(
      SnapshotAccessRequestModel model, SnapshotAccessRequestResponse response) {
    String expectedSummaryString =
        "Participants included:\nName: cohort\nGroups:\nMust meet all of:\nThe following concepts from Race: \nCondition Concept Id: 100\nYear of birth between 1950 and 2000\nTables included:Drug, Condition\n";

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
    assertThat(response.getSummary(), equalToCompressingWhiteSpace(expectedSummaryString));
    assertThat(model.samGroupName(), is(response.getSamGroupName()));
    assertThat(model.samGroupEmail(), is(response.getSamGroupEmail()));
  }
}
