package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.OnDemand;
import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import java.util.List;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
@Category(OnDemand.class)
public class ComplicatedQueryExamplesTest {
  @Test
  void renderComplexSQL() {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    TablePointer conditionOccurrencePointer = TablePointer.fromTableName("condition_occurrence");
    TableVariable conditionOccurrenceVariable =
        TableVariable.forJoined(
            conditionOccurrencePointer,
            "person_id",
            new FieldVariable(new FieldPointer(tablePointer, "person_id"), tableVariable));

    TablePointer conditionAncestorPointer = TablePointer.fromTableName("condition_ancestor");
    TableVariable conditionAncestorVariable =
        TableVariable.forJoined(
            conditionAncestorPointer,
            "ancestor_concept_id",
            new FieldVariable(
                new FieldPointer(conditionOccurrencePointer, "condition_concept_id"),
                conditionOccurrenceVariable));

    Query query =
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(tablePointer, "person_id", "COUNT"),
                    tableVariable,
                    null,
                    true)),
            List.of(tableVariable, conditionOccurrenceVariable, conditionAncestorVariable),
            new BooleanAndOrFilterVariable(
                BooleanAndOrFilterVariable.LogicalOperator.AND,
                List.of(
                    new BooleanAndOrFilterVariable(
                        BooleanAndOrFilterVariable.LogicalOperator.OR,
                        List.of(
                            new BinaryFilterVariable(
                                new FieldVariable(
                                    new FieldPointer(
                                        conditionOccurrencePointer, "condition_concept_id"),
                                    conditionOccurrenceVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(316139)),
                            new BinaryFilterVariable(
                                new FieldVariable(
                                    new FieldPointer(
                                        conditionAncestorPointer, "ancestor_concept_id"),
                                    conditionAncestorVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(316139)),
                            new BinaryFilterVariable(
                                new FieldVariable(
                                    new FieldPointer(
                                        conditionOccurrencePointer, "condition_concept_id"),
                                    conditionOccurrenceVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(4311280)),
                            new BinaryFilterVariable(
                                new FieldVariable(
                                    new FieldPointer(
                                        conditionAncestorPointer, "ancestor_concept_id"),
                                    conditionAncestorVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(4311280)))),
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(tablePointer, "year_of_birth"), tableVariable),
                        BinaryFilterVariable.BinaryOperator.LESS_THAN,
                        new Literal(1983)))));

    assertThat(
        query.renderSQL(null),
        is(
            "SELECT COUNT(DISTINCT p.person_id) FROM person AS p  JOIN condition_occurrence AS c ON c.person_id = p.person_id JOIN condition_ancestor AS c0 ON c0.ancestor_concept_id = c.condition_concept_id WHERE ((c.condition_concept_id = 316139 OR c0.ancestor_concept_id = 316139 OR c.condition_concept_id = 4311280 OR c0.ancestor_concept_id = 4311280) AND p.year_of_birth < 1983)"));
  }
}
