package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.BadRequestException;
import bio.terra.model.CloudPlatform;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CriteriaQueryBuilderTest {
  CriteriaQueryBuilder criteriaQueryBuilder;

  @BeforeEach
  void setup() {
    criteriaQueryBuilder = new CriteriaQueryBuilder("person", s -> s);
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterForRangeCriteria(CloudPlatform platform) {
    SnapshotBuilderProgramDataRangeCriteria rangeCriteria = generateRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(rangeCriteria);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "(null.range_column_name >= 0 AND null.range_column_name <= 100)"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterForListCriteria(CloudPlatform platform) {
    SnapshotBuilderProgramDataListCriteria listCriteria = generateListCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(listCriteria);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace("null.list_column_name IN (0,1,2)"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterForDomainCriteria(CloudPlatform platform) {
    SnapshotBuilderDomainCriteria domainCriteria = generateDomainCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(domainCriteria);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "null.person_id IN (SELECT c.person_id FROM condition_occurrence AS c  JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.condition_concept_id WHERE (c.condition_concept_id = 0 OR c0.ancestor_concept_id = 0))"));
  }

  @Test
  void generateFilterForDomainCriteriaThrowsIfGivenUnknownDomain() {
    SnapshotBuilderDomainCriteria domainCriteria = generateDomainCriteria().domainName("unknown");
    assertThrows(
        BadRequestException.class,
        () -> criteriaQueryBuilder.generateFilter(domainCriteria),
        "Domain unknown is not found in dataset");
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterIdentifiesDomainCriteria(CloudPlatform platform) {
    SnapshotBuilderCriteria criteria = generateDomainCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    // Table name is null because there is no alias generated until it is rendered as a full query
    String sql = filterVariable.renderSQL(CloudPlatformWrapper.of(platform));
    assertThat(
        "It is aliasing for condition occurrence", sql, containsString("SELECT c.person_id FROM"));
    assertThat("Condition occurrence is loaded in", sql, containsString("condition_occurrence"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterIdentifiesRangeCriteria(CloudPlatform platform) {
    SnapshotBuilderCriteria criteria = generateRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        containsString("null.range_column_name >= 0"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterIdentifiesListCriteria(CloudPlatform platform) {
    SnapshotBuilderCriteria criteria = generateListCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        containsString("IN (0,1,2)"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateAndOrFilterForCriteriaGroupHandlesMeetAllTrue(CloudPlatform platform) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(List.of(generateListCriteria(), generateRangeCriteria()))
            .meetAll(true);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateAndOrFilterForCriteriaGroup(criteriaGroup);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "(null.list_column_name IN (0,1,2) AND (null.range_column_name >= 0 AND null.range_column_name <= 100))"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateAndOrFilterForCriteriaGroupHandlesMeetAllFalse(CloudPlatform platform) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(List.of(generateListCriteria(), generateRangeCriteria()))
            .meetAll(false);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateAndOrFilterForCriteriaGroup(criteriaGroup);
    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "(null.list_column_name IN (0,1,2) OR (null.range_column_name >= 0 AND null.range_column_name <= 100))"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterForCriteriaGroupHandlesMustMeetTrue(CloudPlatform platform) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(List.of(generateListCriteria(), generateRangeCriteria()))
            .meetAll(true)
            .mustMeet(true);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateFilterForCriteriaGroup(criteriaGroup);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "(null.list_column_name IN (0,1,2) AND (null.range_column_name >= 0 AND null.range_column_name <= 100))"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterForCriteriaGroupHandlesMustMeetFalse(CloudPlatform platform) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(List.of(generateListCriteria(), generateRangeCriteria()))
            .meetAll(true)
            .mustMeet(false);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateFilterForCriteriaGroup(criteriaGroup);

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "(NOT (null.list_column_name IN (0,1,2) AND (null.range_column_name >= 0 AND null.range_column_name <= 100)))"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateFilterForCriteriaGroups(CloudPlatform platform) {
    FilterVariable filterVariable =
        new CriteriaQueryBuilder("person", null)
            .generateFilterForCriteriaGroups(
                List.of(
                    new SnapshotBuilderCriteriaGroup()
                        .criteria(List.of(generateRangeCriteria()))
                        .meetAll(true)
                        .mustMeet(true),
                    new SnapshotBuilderCriteriaGroup()
                        .criteria(List.of(generateListCriteria()))
                        .meetAll(true)
                        .mustMeet(true)));

    // Table name is null because there is no alias generated until it is rendered as a full query
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "(((null.range_column_name >= 0 AND null.range_column_name <= 100)) AND (null.list_column_name IN (0,1,2)))"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void generateRollupCountsQueryForCriteriaGroupsList(CloudPlatform platform) {
    Query query =
        new CriteriaQueryBuilder("person", s -> s)
            .generateRollupCountsQueryForCriteriaGroupsList(
                List.of(
                    List.of(
                        new SnapshotBuilderCriteriaGroup()
                            .criteria(
                                List.of(
                                    generateDomainCriteria(),
                                    generateListCriteria(),
                                    generateRangeCriteria(),
                                    generateDomainCriteria().domainName("Drug")))
                            .meetAll(true)
                            .mustMeet(true))));
    assertThat(
        "The sql generated is correct",
        query.renderSQL(CloudPlatformWrapper.of(platform)),
        equalToCompressingWhiteSpace(
            "SELECT COUNT(DISTINCT p.person_id) FROM person AS p WHERE (((p.person_id IN (SELECT c.person_id FROM condition_occurrence AS c  JOIN concept_ancestor AS c0 ON c0.ancestor_concept_id = c.condition_concept_id WHERE (c.condition_concept_id = 0 OR c0.ancestor_concept_id = 0)) AND p.list_column_name IN (0,1,2) AND (p.range_column_name >= 0 AND p.range_column_name <= 100) AND p.person_id IN (SELECT d.person_id FROM drug_exposure AS d  JOIN concept_ancestor AS c ON c.ancestor_concept_id = d.drug_concept_id WHERE (d.drug_concept_id = 0 OR c.ancestor_concept_id = 0)))))"));
  }

  private static SnapshotBuilderDomainCriteria generateDomainCriteria() {
    return (SnapshotBuilderDomainCriteria)
        new SnapshotBuilderDomainCriteria()
            .domainName("Condition")
            .id(new BigDecimal(0))
            .name("domain_column_name")
            .kind(SnapshotBuilderCriteria.KindEnum.DOMAIN);
  }

  private static SnapshotBuilderProgramDataRangeCriteria generateRangeCriteria() {
    return (SnapshotBuilderProgramDataRangeCriteria)
        new SnapshotBuilderProgramDataRangeCriteria()
            .low(new BigDecimal(0))
            .high(new BigDecimal(100))
            .id(new BigDecimal(0))
            .name("range_column_name")
            .kind(SnapshotBuilderCriteria.KindEnum.RANGE);
  }

  private static SnapshotBuilderProgramDataListCriteria generateListCriteria() {
    return (SnapshotBuilderProgramDataListCriteria)
        new SnapshotBuilderProgramDataListCriteria()
            .values(List.of(new BigDecimal(0), new BigDecimal(1), new BigDecimal(2)))
            .id(new BigDecimal(0))
            .name("list_column_name")
            .kind(SnapshotBuilderCriteria.KindEnum.LIST);
  }
}
