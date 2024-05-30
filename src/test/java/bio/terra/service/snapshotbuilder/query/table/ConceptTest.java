package bio.terra.service.snapshotbuilder.query.table;

import static bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderTest.assertQueryEquals;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class ConceptTest {
  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testAsPrimary(SqlRenderContext context) {
    Concept concept = Concept.asPrimary();
    assertQueryEquals("concept AS c", concept.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testConceptId(SqlRenderContext context) {
    ConceptAncestor conceptAncestor = ConceptAncestor.forPrimary();
    Concept concept = Concept.joinConceptId(conceptAncestor.ancestorConceptId());
    assertQueryEquals(
        "JOIN concept AS c ON c.concept_id = ca.ancestor_concept_id", concept.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testFieldVariables(SqlRenderContext context) {
    Concept concept = Concept.asPrimary();

    Map<FieldVariable, String> fieldVariablesToResult =
        Map.ofEntries(
            Map.entry(concept.name(), "c.concept_name"),
            Map.entry(concept.conceptId(), "c.concept_id"),
            Map.entry(concept.domainId(), "c.domain_id"),
            Map.entry(concept.conceptCode(), "c.concept_code"),
            Map.entry(concept.standardConcept(), "c.standard_concept"));

    fieldVariablesToResult.forEach(
        (fieldVariable, result) -> {
          assertQueryEquals(result, fieldVariable.renderSQL(context));
        });
  }
}
