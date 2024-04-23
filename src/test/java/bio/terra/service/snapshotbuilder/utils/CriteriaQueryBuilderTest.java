package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.QueryTestUtils;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CriteriaQueryBuilderTest {
  CriteriaQueryBuilder criteriaQueryBuilder;

  @BeforeEach
  void setup() {
    criteriaQueryBuilder =
        new CriteriaQueryBuilder(Person.TABLE_NAME, SnapshotBuilderTestData.SETTINGS);
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterForRangeCriteria(SqlRenderContext context) {
    SnapshotBuilderProgramDataRangeCriteria rangeCriteria = generateYearOfBirthRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(rangeCriteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace("(p.year_of_birth >= 0 AND p.year_of_birth <= 100)"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterForListCriteria(SqlRenderContext context) {
    SnapshotBuilderProgramDataListCriteria listCriteria =
        generateEthnicityListCriteria(List.of(0, 1, 2));
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(listCriteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace("p.ethnicity_concept_id IN (0,1,2)"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterForListCriteriaWithEmptyValues(SqlRenderContext context) {
    SnapshotBuilderProgramDataListCriteria listCriteria = generateEthnicityListCriteria(List.of());
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(listCriteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace("1=1"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterForDomainCriteria(SqlRenderContext context) {
    SnapshotBuilderDomainCriteria domainCriteria =
        generateDomainCriteria(SnapshotBuilderTestData.CONDITION_OCCURRENCE_DOMAIN_ID);
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(domainCriteria);

    String expectedSql =
        "p.person_id IN (SELECT c.person_id FROM condition_occurrence AS c  JOIN concept_ancestor AS c0 ON c0.descendant_concept_id = c.condition_concept_id WHERE c0.ancestor_concept_id = 0)";
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(expectedSql));
  }

  @Test
  void generateFilterForDomainCriteriaThrowsIfGivenUnknownDomain() {
    SnapshotBuilderDomainCriteria domainCriteria = generateDomainCriteria(1000);
    assertThrows(
        BadRequestException.class,
        () -> criteriaQueryBuilder.generateFilter(domainCriteria),
        "Domain unknown is not found in dataset");
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterIdentifiesDomainCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria =
        generateDomainCriteria(SnapshotBuilderTestData.CONDITION_OCCURRENCE_DOMAIN_ID);
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    String sql = filterVariable.renderSQL(context);
    assertThat(
        "The sql generated is correct",
        sql,
        equalToCompressingWhiteSpace(
            "p.person_id IN (SELECT c.person_id FROM condition_occurrence AS c  JOIN concept_ancestor AS c0 ON c0.descendant_concept_id = c.condition_concept_id WHERE c0.ancestor_concept_id = 0)"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterIdentifiesRangeCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria = generateYearOfBirthRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        is("(p.year_of_birth >= 0 AND p.year_of_birth <= 100)"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterIdentifiesListCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria = generateEthnicityListCriteria(List.of(0, 1, 2));
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        is("p.ethnicity_concept_id IN (0,1,2)"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateAndOrFilterForCriteriaGroupHandlesMeetAllTrue(SqlRenderContext context) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(
                List.of(
                    generateEthnicityListCriteria(List.of(0, 1, 2)),
                    generateYearOfBirthRangeCriteria()))
            .meetAll(true);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateAndOrFilterForCriteriaGroup(criteriaGroup);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(
            "(p.ethnicity_concept_id IN (0,1,2) AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100))"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateAndOrFilterForCriteriaGroupHandlesMeetAllFalse(SqlRenderContext context) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(
                List.of(
                    generateEthnicityListCriteria(List.of(0, 1, 2)),
                    generateYearOfBirthRangeCriteria()))
            .meetAll(false);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateAndOrFilterForCriteriaGroup(criteriaGroup);
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(
            "(p.ethnicity_concept_id IN (0,1,2) OR (p.year_of_birth >= 0 AND p.year_of_birth <= 100))"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterForCriteriaGroupHandlesMustMeetTrue(SqlRenderContext context) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(
                List.of(
                    generateEthnicityListCriteria(List.of(0, 1, 2)),
                    generateYearOfBirthRangeCriteria()))
            .meetAll(true)
            .mustMeet(true);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateFilterForCriteriaGroup(criteriaGroup);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(
            "(p.ethnicity_concept_id IN (0,1,2) AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100))"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterForCriteriaGroupHandlesMustMeetFalse(SqlRenderContext context) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(
                List.of(
                    generateEthnicityListCriteria(List.of(0, 1, 2)),
                    generateYearOfBirthRangeCriteria()))
            .meetAll(true)
            .mustMeet(false);
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateFilterForCriteriaGroup(criteriaGroup);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(
            "(NOT (p.ethnicity_concept_id IN (0,1,2) AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100)))"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateFilterForCriteriaGroups(SqlRenderContext context) {
    FilterVariable filterVariable =
        criteriaQueryBuilder.generateFilterForCriteriaGroups(
            List.of(
                new SnapshotBuilderCriteriaGroup()
                    .criteria(List.of(generateYearOfBirthRangeCriteria()))
                    .meetAll(true)
                    .mustMeet(true),
                new SnapshotBuilderCriteriaGroup()
                    .criteria(List.of(generateEthnicityListCriteria(List.of(0, 1, 2))))
                    .meetAll(true)
                    .mustMeet(true)));

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(
            "(((p.year_of_birth >= 0 AND p.year_of_birth <= 100)) AND (p.ethnicity_concept_id IN (0,1,2)))"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void generateRollupCountsQueryForCriteriaGroupsList(SqlRenderContext context) {
    Query query =
        criteriaQueryBuilder.generateRollupCountsQueryForCriteriaGroupsList(
            List.of(
                List.of(
                    new SnapshotBuilderCriteriaGroup()
                        .criteria(
                            List.of(
                                generateDomainCriteria(
                                    SnapshotBuilderTestData.CONDITION_OCCURRENCE_DOMAIN_ID),
                                generateEthnicityListCriteria(List.of(0, 1, 2)),
                                generateYearOfBirthRangeCriteria(),
                                generateDomainCriteria(
                                    SnapshotBuilderTestData.PROCEDURE_OCCURRENCE_DOMAIN_ID)))
                        .meetAll(true)
                        .mustMeet(true))));
    String expectedSql =
        """
      SELECT COUNT(DISTINCT p.person_id)
          FROM person AS p
          WHERE (((p.person_id IN (SELECT c.person_id
            FROM condition_occurrence AS c
            JOIN concept_ancestor AS c0
              ON c0.descendant_concept_id = c.condition_concept_id
            WHERE c0.ancestor_concept_id = 0) AND
              p.ethnicity_concept_id IN (0,1,2)
              AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100)
              AND p.person_id IN (SELECT p0.person_id
            FROM procedure_occurrence AS p0
              JOIN concept_ancestor AS c1
              ON c1.descendant_concept_id = p0.procedure_concept_id
            WHERE c1.ancestor_concept_id = 0))))
    """;
    assertThat(
        "The sql generated is correct",
        query.renderSQL(context),
        equalToCompressingWhiteSpace(expectedSql));
  }

  private static SnapshotBuilderDomainCriteria generateDomainCriteria(int domainId) {
    return (SnapshotBuilderDomainCriteria)
        new SnapshotBuilderDomainCriteria()
            .conceptId(0)
            .id(domainId)
            .kind(SnapshotBuilderCriteria.KindEnum.DOMAIN);
  }

  private static SnapshotBuilderProgramDataRangeCriteria generateYearOfBirthRangeCriteria() {
    return (SnapshotBuilderProgramDataRangeCriteria)
        new SnapshotBuilderProgramDataRangeCriteria()
            .low(0)
            .high(100)
            .id(1)
            .kind(SnapshotBuilderCriteria.KindEnum.RANGE);
  }

  private static SnapshotBuilderProgramDataListCriteria generateEthnicityListCriteria(
      List<Integer> values) {
    return (SnapshotBuilderProgramDataListCriteria)
        new SnapshotBuilderProgramDataListCriteria()
            .values(values)
            .id(2)
            .kind(SnapshotBuilderCriteria.KindEnum.LIST);
  }
}
