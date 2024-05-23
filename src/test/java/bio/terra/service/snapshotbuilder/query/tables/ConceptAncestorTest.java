package bio.terra.service.snapshotbuilder.query.tables;

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
public class ConceptAncestorTest {
  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testAsPrimary(SqlRenderContext context) {
    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    assertQueryEquals("concept_ancestor AS ca", conceptAncestor.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testJoinDescendant(SqlRenderContext context) {
    ConceptAncestor conceptAncestor =
        ConceptAncestor.joinDescendant(ConceptRelationship.asPrimary().relationship_id());
    assertQueryEquals(
        "JOIN concept_ancestor AS ca ON ca.descendant_concept_id = cr.relationship_id",
        conceptAncestor.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testJoinAncestor(SqlRenderContext context) {
    ConceptAncestor conceptAncestor =
        ConceptAncestor.joinAncestor(ConceptRelationship.asPrimary().relationship_id());

    assertQueryEquals(
        "JOIN concept_ancestor AS ca ON ca.ancestor_concept_id = cr.relationship_id",
        conceptAncestor.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testFieldVariables(SqlRenderContext context) {
    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    Map<FieldVariable, String> fieldVariablesToResult =
        Map.ofEntries(
            Map.entry(conceptAncestor.ancestor_concept_id(), "ca.ancestor_concept_id"),
            Map.entry(conceptAncestor.descendant_concept_id(), "ca.descendant_concept_id"));

    fieldVariablesToResult.forEach(
        (fieldVariable, result) -> {
          assertQueryEquals(result, fieldVariable.renderSQL(context));
        });
  }
}
