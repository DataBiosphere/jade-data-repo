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
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
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
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForRangeCriteria(SqlRenderContext context) {
    SnapshotBuilderProgramDataRangeCriteria rangeCriteria = generateRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(rangeCriteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace("(p.year_of_birth >= 0 AND p.year_of_birth <= 100)"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForListCriteria(SqlRenderContext context) {
    SnapshotBuilderProgramDataListCriteria listCriteria = generateListCriteria(List.of(0, 1, 2));
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(listCriteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace("p.ethnicity_concept_id IN (0,1,2)"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForListCriteriaWithEmptyValues(SqlRenderContext context) {
    SnapshotBuilderProgramDataListCriteria listCriteria = generateListCriteria(List.of());
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(listCriteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace("1=1"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForDomainCriteria(SqlRenderContext context) {
    SnapshotBuilderDomainCriteria domainCriteria = generateDomainCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilter(domainCriteria);

    String expectedSql =
        "p.person_id IN (SELECT co.person_id FROM condition_occurrence AS co  JOIN concept_ancestor AS ca ON ca.descendant_concept_id = co.condition_concept_id WHERE ca.ancestor_concept_id = 0)";
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(expectedSql));
  }

  @Test
  void generateFilterForDomainCriteriaThrowsIfGivenUnknownDomain() {
    SnapshotBuilderDomainCriteria domainCriteria =
        (SnapshotBuilderDomainCriteria) generateDomainCriteria().id(1000);
    assertThrows(
        BadRequestException.class,
        () -> criteriaQueryBuilder.generateFilter(domainCriteria),
        "Domain unknown is not found in dataset");
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterIdentifiesDomainCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria = generateDomainCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    String sql = filterVariable.renderSQL(context);
    assertThat(
        "The sql generated is correct",
        sql,
        equalToCompressingWhiteSpace(
            "p.person_id IN (SELECT co.person_id FROM condition_occurrence AS co JOIN concept_ancestor AS ca ON ca.descendant_concept_id = co.condition_concept_id WHERE ca.ancestor_concept_id = 0)"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterIdentifiesRangeCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria = generateRangeCriteria();
    FilterVariable filterVariable = criteriaQueryBuilder.generateFilterForCriteria(criteria);

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        is("(p.year_of_birth >= 0 AND p.year_of_birth <= 100)"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterIdentifiesListCriteria(SqlRenderContext context) {
    SnapshotBuilderCriteria criteria = generateListCriteria(List.of(0, 1, 2));
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
            .criteria(List.of(generateListCriteria(List.of(0, 1, 2)), generateRangeCriteria()))
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
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateAndOrFilterForCriteriaGroupHandlesMeetAllFalse(SqlRenderContext context) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(List.of(generateListCriteria(List.of(0, 1, 2)), generateRangeCriteria()))
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
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForCriteriaGroupHandlesMustMeetTrue(SqlRenderContext context) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(List.of(generateListCriteria(List.of(0, 1, 2)), generateRangeCriteria()))
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
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForCriteriaGroupHandlesMustMeetFalse(SqlRenderContext context) {
    SnapshotBuilderCriteriaGroup criteriaGroup =
        new SnapshotBuilderCriteriaGroup()
            .criteria(List.of(generateListCriteria(List.of(0, 1, 2)), generateRangeCriteria()))
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
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateFilterForCriteriaGroups(SqlRenderContext context) {
    FilterVariable filterVariable =
        new CriteriaQueryBuilder(Person.TABLE_NAME, SnapshotBuilderTestData.SETTINGS)
            .generateFilterForCriteriaGroups(
                List.of(
                    new SnapshotBuilderCriteriaGroup()
                        .criteria(List.of(generateRangeCriteria()))
                        .meetAll(true)
                        .mustMeet(true),
                    new SnapshotBuilderCriteriaGroup()
                        .criteria(List.of(generateListCriteria(List.of(0, 1, 2))))
                        .meetAll(true)
                        .mustMeet(true)));

    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(context),
        equalToCompressingWhiteSpace(
            "(((p.year_of_birth >= 0 AND p.year_of_birth <= 100)) AND (p.ethnicity_concept_id IN (0,1,2)))"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void generateRollupCountsQueryForCriteriaGroupsList(SqlRenderContext context) {
    Query query =
        new CriteriaQueryBuilder(Person.TABLE_NAME, SnapshotBuilderTestData.SETTINGS)
            .generateRollupCountsQueryForCriteriaGroupsList(
                List.of(
                    List.of(
                        new SnapshotBuilderCriteriaGroup()
                            .criteria(
                                List.of(
                                    generateDomainCriteria(),
                                    generateListCriteria(List.of(0, 1, 2)),
                                    generateRangeCriteria(),
                                    generateDomainCriteria().id(11)))
                            .meetAll(true)
                            .mustMeet(true))));
    // FIXME: is query correct? It doesn't contain the concept IDs 11 and 10.
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
    assertThat(
        "The sql generated is correct",
        query.renderSQL(context),
        equalToCompressingWhiteSpace(expectedSql));
  }

  private static SnapshotBuilderDomainCriteria generateDomainCriteria() {
    return (SnapshotBuilderDomainCriteria)
        new SnapshotBuilderDomainCriteria()
            .conceptId(0)
            .id(10)
            .name("domain_column_name")
            .kind(SnapshotBuilderCriteria.KindEnum.DOMAIN);
  }

  private static SnapshotBuilderProgramDataRangeCriteria generateRangeCriteria() {
    return (SnapshotBuilderProgramDataRangeCriteria)
        new SnapshotBuilderProgramDataRangeCriteria()
            .low(0)
            .high(100)
            .id(1)
            .name("range_column_name")
            .kind(SnapshotBuilderCriteria.KindEnum.RANGE);
  }

  private static SnapshotBuilderProgramDataListCriteria generateListCriteria(List<Integer> values) {
    return (SnapshotBuilderProgramDataListCriteria)
        new SnapshotBuilderProgramDataListCriteria()
            .values(values)
            .id(2)
            .name("list_column_name")
            .kind(SnapshotBuilderCriteria.KindEnum.LIST);
  }
}
