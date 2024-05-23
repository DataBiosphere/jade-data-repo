package bio.terra.service.snapshotbuilder.query.tables;

import static bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderTest.assertQueryEquals;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class DomainOccurrenceTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testDomainOccurrence(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption.root(new SnapshotBuilderConcept()).tableName("table");

    DomainOccurrence domainOccurrence = DomainOccurrence.forPrimary(domainOption);

    assertQueryEquals("table AS t", domainOccurrence.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testLeftJoinOn(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption
        .root(new SnapshotBuilderConcept())
        .tableName(ConditionOccurrence.TABLE_NAME)
        .columnName(ConditionOccurrence.CONDITION_CONCEPT_ID);

    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    DomainOccurrence domainOccurrence =
        DomainOccurrence.leftJoinOn(domainOption, conceptAncestor.descendantConceptId());
    assertQueryEquals(
        "LEFT JOIN condition_occurrence AS co ON co.condition_concept_id = ca.descendant_concept_id",
        domainOccurrence.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testCountPersonId(SqlRenderContext context) {
    SnapshotBuilderDomainOption domainOption = new SnapshotBuilderDomainOption();
    domainOption
        .root(new SnapshotBuilderConcept())
        .tableName(ConditionOccurrence.TABLE_NAME)
        .columnName(ConditionOccurrence.CONDITION_CONCEPT_ID);
    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    DomainOccurrence domainOccurrence =
        DomainOccurrence.leftJoinOn(domainOption, conceptAncestor.descendantConceptId());
    FieldVariable countPersonId = domainOccurrence.countPersonId();
    assertQueryEquals("COUNT(DISTINCT co.person_id) AS count", countPersonId.renderSQL(context));
  }
}
