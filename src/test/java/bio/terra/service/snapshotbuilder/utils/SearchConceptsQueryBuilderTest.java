package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder.createDomainClause;
import static bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder.createSearchConceptClause;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SearchConceptsQueryBuilderTest {

  @Test
  void buildSearchConceptsQuery() {
    assertThat(
        "generated SQL is correct",
        SearchConceptsQueryBuilder.buildSearchConceptsQuery("condition", "cancer", s -> s),
        equalToCompressingWhiteSpace(
            "SELECT c.concept_name, c.concept_id, COUNT(DISTINCT c0.person_id) "
                + "FROM concept AS c  "
                + "JOIN condition_occurrence AS c0 "
                + "ON c0.condition_concept_id = c.concept_id "
                + "WHERE (c.domain_id = 'condition' "
                + "AND (CONTAINS_SUBSTR(c.concept_name, 'cancer') "
                + "OR CONTAINS_SUBSTR(c.concept_code, 'cancer'))) "
                + "LIMIT 100"));
  }

  @Test
  void testCreateSearchConceptClause() {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", s -> s);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);

    assertThat(
        "generated sql is as expected",
        createSearchConceptClause(
                conceptTablePointer, conceptTableVariable, "cancer", "concept_name")
            .renderSQL(),
        // table name is added when the Query is created
        equalTo("CONTAINS_SUBSTR(null.concept_name, 'cancer')"));
  }

  @Test
  void testCreateDomainClause() {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", s -> s);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);

    assertThat(
        "generated sql is as expected",
        createDomainClause(conceptTablePointer, conceptTableVariable, "cancer").renderSQL(),
        // table name is added when the Query is created
        equalTo("null.domain_id = 'cancer'"));
  }
}
