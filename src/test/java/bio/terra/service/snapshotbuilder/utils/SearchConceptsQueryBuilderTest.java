package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder.createSearchConceptClause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.utils.constants.Concept;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SearchConceptsQueryBuilderTest {

  private SnapshotBuilderDomainOption createDomainOption(
      String name, int id, String occurrenceTable, String columnName) {
    var option = new SnapshotBuilderDomainOption();
    option.name(name).id(id).tableName(occurrenceTable).columnName(columnName);
    return option;
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildSearchConceptsQuery(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Observation", 27, "observation", "observation_concept_id");

    String expectedGcp =
        """
            SELECT
              c.concept_name,
              c.concept_id,
              c.concept_code,
              COUNT(DISTINCT o.person_id) AS count,
              true AS has_children
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
              ((c.domain_id = 'Observation'
                  AND c.standard_concept = 'S')
                AND (CONTAINS_SUBSTR(c.concept_name, 'cancer')
                  OR CONTAINS_SUBSTR(c.concept_code, 'cancer')))
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
              COUNT(DISTINCT o.person_id) AS count,
              1 AS has_children
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
              ((c.domain_id = 'Observation'
                  AND c.standard_concept = 'S')
                AND (CHARINDEX('cancer',
                    c.concept_name) > 0
                  OR CHARINDEX('cancer',
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
            .searchConceptsQueryBuilder()
            .buildSearchConceptsQuery(domainOption, "cancer")
            .renderSQL(context);

    assertThat(
        "generated SQL is correct",
        actual,
        equalToCompressingWhiteSpace(context.getPlatform().choose(expectedGcp, expectedAzure)));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void buildSearchConceptsQueryEmpty(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption =
        createDomainOption("Condition", 19, "condition_occurrence", "condition_concept_id");
    String actual =
        new QueryBuilderFactory()
            .searchConceptsQueryBuilder()
            .buildSearchConceptsQuery(domainOption, "")
            .renderSQL(context);
    String gcpExpected =
        """
        SELECT c.concept_name, c.concept_id, c.concept_code, COUNT(DISTINCT co.person_id) AS count, true AS has_children
        FROM concept AS c
          JOIN concept_ancestor AS ca ON ca.ancestor_concept_id = c.concept_id
          LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca.descendant_concept_id
        WHERE (c.domain_id = 'Condition' AND c.standard_concept = 'S')
        GROUP BY c.concept_name, c.concept_id, c.concept_code
        ORDER BY count DESC
        LIMIT 100""";

    String azureExpected =
        """
        SELECT TOP 100 c.concept_name, c.concept_id, c.concept_code, COUNT(DISTINCT co.person_id) AS count, 1 AS has_children
        FROM concept AS c
          JOIN concept_ancestor AS ca ON ca.ancestor_concept_id = c.concept_id
          LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca.descendant_concept_id
        WHERE (c.domain_id = 'Condition' AND c.standard_concept = 'S')
        GROUP BY c.concept_name, c.concept_id, c.concept_code
        ORDER BY count DESC""";

    assertThat(
        "generated SQL for GCP and Azure empty search string is correct",
        actual,
        equalToCompressingWhiteSpace(context.getPlatform().choose(gcpExpected, azureExpected)));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testCreateSearchConceptClause(SqlRenderContext context) {
    CloudPlatformWrapper platformWrapper = context.getPlatform();
    TableVariable conceptTableVariable =
        TableVariable.forPrimary(TablePointer.fromTableName(Concept.TABLE_NAME));
    String actual =
        createSearchConceptClause(conceptTableVariable, "cancer", Concept.CONCEPT_NAME)
            .renderSQL(context);

    var expectedGCPQuery = "CONTAINS_SUBSTR(c.concept_name, 'cancer')";
    var expectedAzureQuery = "CHARINDEX('cancer', c.concept_name) > 0";

    assertThat(
        "generated sql is as expected",
        actual,
        // table name is added when the Query is created
        equalToCompressingWhiteSpace(
            platformWrapper.choose(() -> expectedGCPQuery, () -> expectedAzureQuery)));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testCreateDomainClause(SqlRenderContext context) {
    TableVariable conceptTableVariable =
        TableVariable.forPrimary(TablePointer.fromTableName(Concept.TABLE_NAME));

    assertThat(
        "generated sql is as expected",
        SearchConceptsQueryBuilder.createDomainClause(conceptTableVariable, "domain")
            .renderSQL(context),
        is("(c.domain_id = 'domain' AND c.standard_concept = 'S')"));
  }
}
