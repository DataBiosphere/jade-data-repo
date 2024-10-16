package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class CriteriaQueryBuilderTest {
  private CriteriaQueryBuilder criteriaQueryBuilder;

  @BeforeEach
  void setup() {
    criteriaQueryBuilder =
        new QueryBuilderFactory().criteriaQueryBuilder(SnapshotBuilderTestData.SETTINGS);
  }

  public static String stripSpaces(String toBeStripped) {
    return toBeStripped.replaceAll("\\s+", " ").trim();
  }

  public static void assertQueryEquals(String expectedSql, String actualSql) {
    assertEquals(stripSpaces(expectedSql), stripSpaces(actualSql), "The sql generated is correct");
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForRangeCriteria(SqlRenderContext context) {
    SnapshotBuilderProgramDataRangeCriteria rangeCriteria = generateYearOfBirthRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(rangeCriteria);

    assertQueryEquals(
        "(p.year_of_birth >= 0 AND p.year_of_birth <= 100)", filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForListCriteria(SqlRenderContext context) {
    SnapshotBuilderProgramDataListCriteria listCriteria =
        generateEthnicityListCriteria(List.of(0, 1, 2));
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(listCriteria);

    assertQueryEquals("p.ethnicity_concept_id IN (0,1,2)", filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForListCriteriaWithEmptyValues(SqlRenderContext context) {
    SnapshotBuilderProgramDataListCriteria listCriteria = generateEthnicityListCriteria(List.of());
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(listCriteria);

    assertQueryEquals("1=1", filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForDomainCriteria(SqlRenderContext context) {
    SnapshotBuilderDomainCriteria domainCriteria =
        generateDomainCriteria(SnapshotBuilderTestData.CONDITION_OCCURRENCE_DOMAIN_ID);
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(domainCriteria);

    String expectedSql =
        "p.person_id IN (SELECT co.person_id FROM condition_occurrence AS co  JOIN concept_ancestor AS ca ON ca.descendant_concept_id = co.condition_concept_id WHERE ca.ancestor_concept_id = 0)";
    assertQueryEquals(expectedSql, filterVariable.renderSQL(context));
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
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterIdentifiesDomainCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria =
        generateDomainCriteria(SnapshotBuilderTestData.CONDITION_OCCURRENCE_DOMAIN_ID);
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    assertQueryEquals(
        "p.person_id IN (SELECT co.person_id FROM condition_occurrence AS co JOIN concept_ancestor AS ca ON ca.descendant_concept_id = co.condition_concept_id WHERE ca.ancestor_concept_id = 0)",
        filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterIdentifiesRangeCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria = generateYearOfBirthRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        is("(p.year_of_birth >= 0 AND p.year_of_birth <= 100)"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterIdentifiesListCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria = generateEthnicityListCriteria(List.of(0, 1, 2));
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        is("p.ethnicity_concept_id IN (0,1,2)"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
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

    assertQueryEquals(
        "(p.ethnicity_concept_id IN (0,1,2) AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100))",
        filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
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
    assertQueryEquals(
        "(p.ethnicity_concept_id IN (0,1,2) OR (p.year_of_birth >= 0 AND p.year_of_birth <= 100))",
        filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
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

    assertQueryEquals(
        "(p.ethnicity_concept_id IN (0,1,2) AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100))",
        filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
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

    assertQueryEquals(
        "(NOT (p.ethnicity_concept_id IN (0,1,2) AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100)))",
        filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
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

    assertQueryEquals(
        "(((p.year_of_birth >= 0 AND p.year_of_birth <= 100)) AND (p.ethnicity_concept_id IN (0,1,2)))",
        filterVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateRollupCountsQueryForCohorts(SqlRenderContext context) {
    Query query =
        criteriaQueryBuilder.generateRollupCountsQueryForCohorts(
            List.of(
                new SnapshotBuilderCohort()
                    .criteriaGroups(
                        List.of(
                            new SnapshotBuilderCriteriaGroup()
                                .criteria(
                                    List.of(
                                        generateDomainCriteria(
                                            SnapshotBuilderTestData.CONDITION_OCCURRENCE_DOMAIN_ID),
                                        generateEthnicityListCriteria(List.of(0, 1, 2)),
                                        generateYearOfBirthRangeCriteria(),
                                        generateDomainCriteria(
                                            SnapshotBuilderTestData
                                                .PROCEDURE_OCCURRENCE_DOMAIN_ID)))
                                .meetAll(true)
                                .mustMeet(true)))));
    String expectedSql =
        """
        SELECT COUNT(DISTINCT p.person_id)
            FROM person AS p
            WHERE (((p.person_id IN (SELECT co.person_id
              FROM condition_occurrence AS co
              JOIN concept_ancestor AS ca
                ON ca.descendant_concept_id = co.condition_concept_id
              WHERE ca.ancestor_concept_id = 0) AND
                p.ethnicity_concept_id IN (0,1,2)
                AND (p.year_of_birth >= 0 AND p.year_of_birth <= 100)
                AND p.person_id IN (SELECT po.person_id
              FROM procedure_occurrence AS po
                JOIN concept_ancestor AS ca1
                ON ca1.descendant_concept_id = po.procedure_concept_id
              WHERE ca1.ancestor_concept_id = 0))))""";
    assertQueryEquals(expectedSql, query.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateRowIdQueryForCohorts(SqlRenderContext context) {
    Query query =
        criteriaQueryBuilder.generateRowIdQueryForCohorts(
            SnapshotBuilderTestData.createSnapshotAccessRequest(UUID.randomUUID())
                .getSnapshotBuilderRequest()
                .getCohorts());
    String expectedSql =
        """
    SELECT p.datarepo_row_id FROM person AS p WHERE
        (((1=1 AND p.person_id IN
            (SELECT co.person_id FROM condition_occurrence AS co
            JOIN concept_ancestor AS ca ON ca.descendant_concept_id = co.condition_concept_id
            WHERE ca.ancestor_concept_id = 100)
            AND (p.year_of_birth >= 1950 AND p.year_of_birth <= 2000))))
    """;
    assertQueryEquals(expectedSql, query.renderSQL(context));
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
