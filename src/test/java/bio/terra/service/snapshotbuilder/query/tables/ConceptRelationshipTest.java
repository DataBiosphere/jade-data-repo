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
public class ConceptRelationshipTest {
  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testAsPrimary(SqlRenderContext context) {
    ConceptRelationship conceptRelationship = ConceptRelationship.asPrimary();
    assertQueryEquals("concept_relationship AS cr", conceptRelationship.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testFieldVariables(SqlRenderContext context) {
    ConceptRelationship conceptRelationship = ConceptRelationship.asPrimary();
    Map<FieldVariable, String> fieldVariablesToResult =
        Map.ofEntries(
            Map.entry(conceptRelationship.concept_id_1(), "cr.concept_id_1"),
            Map.entry(conceptRelationship.concept_id_2(), "cr.concept_id_2"),
            Map.entry(conceptRelationship.relationship_id(), "cr.relationship_id"));

    fieldVariablesToResult.forEach(
        (fieldVariable, result) -> {
          assertQueryEquals(result, fieldVariable.renderSQL(context));
        });
  }
}
