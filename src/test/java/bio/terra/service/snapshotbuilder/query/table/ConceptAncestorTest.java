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
class ConceptAncestorTest {
  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testAsPrimary(SqlRenderContext context) {
    ConceptAncestor conceptAncestor = ConceptAncestor.forPrimary();
    assertQueryEquals("concept_ancestor AS ca", conceptAncestor.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testJoinDescendant(SqlRenderContext context) {
    ConceptAncestor conceptAncestor =
        ConceptAncestor.joinDescendant(ConceptRelationship.forPrimary().relationshipId());
    assertQueryEquals(
        "JOIN concept_ancestor AS ca ON ca.descendant_concept_id = cr.relationship_id",
        conceptAncestor.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testJoinAncestor(SqlRenderContext context) {
    ConceptAncestor conceptAncestor =
        ConceptAncestor.joinAncestor(ConceptRelationship.forPrimary().relationshipId());

    assertQueryEquals(
        "JOIN concept_ancestor AS ca ON ca.ancestor_concept_id = cr.relationship_id",
        conceptAncestor.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testFieldVariables(SqlRenderContext context) {
    ConceptAncestor conceptAncestor = ConceptAncestor.forPrimary();
    Map<FieldVariable, String> fieldVariablesToResult =
        Map.of(
            conceptAncestor.ancestorConceptId(), "ca.ancestor_concept_id",
            conceptAncestor.descendantConceptId(), "ca.descendant_concept_id");

    fieldVariablesToResult.forEach(
        (fieldVariable, result) -> {
          assertQueryEquals(result, fieldVariable.renderSQL(context));
        });
  }
}
