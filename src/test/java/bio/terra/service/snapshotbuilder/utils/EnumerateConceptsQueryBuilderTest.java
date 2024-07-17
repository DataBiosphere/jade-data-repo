package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderTest.assertQueryEquals;
import static bio.terra.service.snapshotbuilder.utils.EnumerateConceptsQueryBuilder.createFilterConceptClause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderColumn;
import bio.terra.model.SnapshotBuilderColumnFilterDetails;
import bio.terra.model.SnapshotBuilderColumnFilterType;
import bio.terra.model.SnapshotBuilderColumnOrderByDirection;
import bio.terra.model.SnapshotBuilderColumnSelectDetails;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderEnumerateModel;
import bio.terra.model.SnapshotBuilderJoinModel;
import bio.terra.model.SnapshotBuilderJoinTable;
import bio.terra.model.SnapshotBuilderJoinToTable;
import bio.terra.model.SnapshotBuilderPrimaryTable;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class EnumerateConceptsQueryBuilderTest {

  private SnapshotBuilderDomainOption createDomainOption(
      String name, int id, String occurrenceTable, String columnName) {
    var option = new SnapshotBuilderDomainOption();
    SnapshotBuilderJoinToTable occurrence = new SnapshotBuilderJoinToTable();
    occurrence
        .columnsUsed(
            List.of(
                new SnapshotBuilderColumn()
                    .name("person_id")
                    .selectDetails(
                        new SnapshotBuilderColumnSelectDetails()
                            .isDistinct(true)
                            .alias("count")
                            .functionWrapper("COUNT"))
                    .orderByDetails(SnapshotBuilderColumnOrderByDirection.DESCENDING)))
        .table(occurrenceTable)
        .column(columnName);
    SnapshotBuilderJoinToTable conceptAncestor = new SnapshotBuilderJoinToTable();
    conceptAncestor.columnsUsed(List.of()).table("concept_ancestor").column("ancestor_concept_id");
    option
        .enumerate(
            new SnapshotBuilderEnumerateModel()
                .startsWith(
                    new SnapshotBuilderPrimaryTable()
                        .name("concept")
                        .columnsUsed(
                            List.of(
                                new SnapshotBuilderColumn()
                                    .name("concept_name")
                                    .selectDetails(
                                        new SnapshotBuilderColumnSelectDetails().isDistinct(false))
                                    .filterDetails(
                                        List.of(
                                            new SnapshotBuilderColumnFilterDetails()
                                                .type(SnapshotBuilderColumnFilterType.SEARCH_TERM)))
                                    .isGroupBy(true),
                                new SnapshotBuilderColumn()
                                    .name("concept_id")
                                    .selectDetails(
                                        new SnapshotBuilderColumnSelectDetails().isDistinct(false))
                                    .isGroupBy(true),
                                new SnapshotBuilderColumn()
                                    .name("concept_code")
                                    .selectDetails(
                                        new SnapshotBuilderColumnSelectDetails().isDistinct(false))
                                    .filterDetails(
                                        List.of(
                                            new SnapshotBuilderColumnFilterDetails()
                                                .type(SnapshotBuilderColumnFilterType.SEARCH_TERM)))
                                    .isGroupBy(true),
                                new SnapshotBuilderColumn()
                                    .name("domain_id")
                                    .filterDetails(
                                        List.of(
                                            new SnapshotBuilderColumnFilterDetails()
                                                .type(SnapshotBuilderColumnFilterType.LITERAL)
                                                .literal(name))),
                                new SnapshotBuilderColumn()
                                    .name("standard_concept")
                                    .filterDetails(
                                        List.of(
                                            new SnapshotBuilderColumnFilterDetails()
                                                .type(SnapshotBuilderColumnFilterType.LITERAL)
                                                .literal("S"))))))
                .joinTables(
                    List.of(
                        new SnapshotBuilderJoinModel()
                            .leftJoin(false)
                            .from(
                                new SnapshotBuilderJoinTable()
                                    .table("concept")
                                    .column("concept_id"))
                            .to(conceptAncestor),
                        new SnapshotBuilderJoinModel()
                            .leftJoin(true)
                            .from(
                                new SnapshotBuilderJoinTable()
                                    .table("concept_ancestor")
                                    .column("descendant_concept_id"))
                            .to(occurrence))))
        .name(name)
        .id(id)
        .tableName(occurrenceTable)
        .columnName(columnName);
    return option;
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildEnumerateConceptsQuery(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Observation", 27, "observation", "observation_concept_id");

    String expectedGcp =
        """
            SELECT
              c.concept_name,
              c.concept_id,
              c.concept_code,
              COUNT(DISTINCT o.person_id) AS count
            FROM
              concept AS c
            JOIN
              concept_ancestor AS ca
            ON
              ca.ancestor_concept_id = c.concept_id
            LEFT JOIN
              observation AS o
            ON
              o.observation_concept_id = ca.descendant_concept_id
            WHERE
              (c.domain_id = 'Observation'
                  AND c.standard_concept = 'S'
                AND (CONTAINS_SUBSTR(c.concept_name, @filter_text)
                  OR CONTAINS_SUBSTR(c.concept_code, @filter_text)))
            GROUP BY
              c.concept_name,
              c.concept_id,
              c.concept_code
            ORDER BY
              count DESC
            LIMIT
              100
        """;

    String expectedAzure =
        """
            SELECT
              TOP 100 c.concept_name,
              c.concept_id,
              c.concept_code,
              COUNT(DISTINCT o.person_id) AS count
            FROM
              concept AS c
            JOIN
              concept_ancestor AS ca
            ON
              ca.ancestor_concept_id = c.concept_id
            LEFT JOIN
              observation AS o
            ON
              o.observation_concept_id = ca.descendant_concept_id
            WHERE
              (c.domain_id = 'Observation'
                  AND c.standard_concept = 'S'
                AND (CHARINDEX(:filter_text,
                    c.concept_name) > 0
                  OR CHARINDEX(:filter_text,
                    c.concept_code) > 0))
            GROUP BY
              c.concept_name,
              c.concept_id,
              c.concept_code
            ORDER BY
              count DESC
        """;

    String actual =
        new QueryBuilderFactory()
            .enumerateConceptsQueryBuilder()
            .buildEnumerateConceptsQuery(domainOption, true)
            .renderSQL(context);

    assertQueryEquals(context.getPlatform().choose(expectedGcp, expectedAzure), actual);
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildEnumerateConceptsQueryEmpty(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Condition", 19, "condition_occurrence", "condition_concept_id");
    String actual =
        new QueryBuilderFactory()
            .enumerateConceptsQueryBuilder()
            .buildEnumerateConceptsQuery(domainOption, false)
            .renderSQL(context);
    String gcpExpected =
        """
        SELECT c.concept_name, c.concept_id, c.concept_code, COUNT(DISTINCT co.person_id) AS count
        FROM concept AS c
          JOIN concept_ancestor AS ca ON ca.ancestor_concept_id = c.concept_id
          LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca.descendant_concept_id
        WHERE (c.domain_id = 'Condition' AND c.standard_concept = 'S')
        GROUP BY c.concept_name, c.concept_id, c.concept_code
        ORDER BY count DESC
        LIMIT 100""";

    String azureExpected =
        """
        SELECT TOP 100 c.concept_name, c.concept_id, c.concept_code, COUNT(DISTINCT co.person_id) AS count
        FROM concept AS c
          JOIN concept_ancestor AS ca ON ca.ancestor_concept_id = c.concept_id
          LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca.descendant_concept_id
        WHERE (c.domain_id = 'Condition' AND c.standard_concept = 'S')
        GROUP BY c.concept_name, c.concept_id, c.concept_code
        ORDER BY count DESC""";

    assertQueryEquals(context.getPlatform().choose(gcpExpected, azureExpected), actual);
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testCreateFilterConceptClause(SqlRenderContext context) {
    Concept concept = Concept.asPrimary();
    String actual = createFilterConceptClause(concept.name()).renderSQL(context);

    var expectedGCPQuery = "CONTAINS_SUBSTR(c.concept_name, @filter_text)";
    var expectedAzureQuery = "CHARINDEX(:filter_text, c.concept_name) > 0";

    assertQueryEquals(context.getPlatform().choose(expectedGCPQuery, expectedAzureQuery), actual);
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testCreateDomainClause(SqlRenderContext context) {
    Concept concept = Concept.asPrimary();
    assertThat(
        "generated sql is as expected",
        EnumerateConceptsQueryBuilder.createDomainClause(concept, "domain").renderSQL(context),
        is("(c.domain_id = 'domain' AND c.standard_concept = 'S')"));
  }
}
