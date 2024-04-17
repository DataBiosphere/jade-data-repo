package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptAncestorConstants;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrenceConstants;
import bio.terra.service.snapshotbuilder.utils.constants.PersonConstants;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
public class QueryTest {

  @NotNull
  public static Query createQuery() {
    TableVariable tableVariable = makeTableVariable();
    return new Query(
        List.of(
            new FieldVariable(
                FieldPointer.allFields(tableVariable.getTablePointer()), tableVariable)),
        List.of(tableVariable));
  }

  @NotNull
  public static Query createQueryWithLimit() {
    TableVariable tableVariable = makeTableVariable();
    return new Query(
        List.of(
            new FieldVariable(
                FieldPointer.allFields(tableVariable.getTablePointer()), tableVariable)),
        List.of(tableVariable),
        null,
        25);
  }

  private static TableVariable makeTableVariable() {
    TablePointer tablePointer = QueryTestUtils.fromTableName("table");
    return TableVariable.forPrimary(tablePointer);
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderSQL(SqlRenderContext context) {
    assertThat(createQuery().renderSQL(context), is("SELECT t.* FROM table AS t"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderSQLWithLimit(SqlRenderContext context) {
    String query = "t.* FROM table AS t";
    String expected =
        context.getPlatform().choose("SELECT " + query + " LIMIT 25", "SELECT TOP 25 " + query);
    assertThat(createQueryWithLimit().renderSQL(context), is(expected));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderSqlGroupBy(SqlRenderContext context) {
    TablePointer tablePointer = QueryTestUtils.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    FieldPointer fieldPointer = new FieldPointer(tablePointer, "field");
    FieldVariable fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    Query query = new Query(List.of(fieldVariable), List.of(tableVariable), List.of(fieldVariable));
    assertThat(query.renderSQL(context), is("SELECT t.field FROM table AS t GROUP BY t.field"));
  }

  @ParameterizedTest
  @ArgumentsSource(QueryTestUtils.Contexts.class)
  void renderComplexSQL(SqlRenderContext context) {
    TablePointer tablePointer = QueryTestUtils.fromTableName(PersonConstants.PERSON);
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    TablePointer conditionOccurrencePointer =
        QueryTestUtils.fromTableName(ConditionOccurrenceConstants.CONDITION_OCCURRENCE);
    TableVariable conditionOccurrenceVariable =
        TableVariable.forJoined(
            conditionOccurrencePointer,
            PersonConstants.PERSON_ID,
            new FieldVariable(
                new FieldPointer(tablePointer, PersonConstants.PERSON_ID), tableVariable));

    TablePointer conditionAncestorPointer = QueryTestUtils.fromTableName("condition_ancestor");
    TableVariable conditionAncestorVariable =
        TableVariable.forJoined(
            conditionAncestorPointer,
            ConceptAncestorConstants.ANCESTOR_CONCEPT_ID,
            new FieldVariable(
                new FieldPointer(
                    conditionOccurrencePointer, ConditionOccurrenceConstants.CONDITION_CONCEPT_ID),
                conditionOccurrenceVariable));

    Query query =
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(tablePointer, PersonConstants.PERSON_ID, "COUNT"),
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
                                        conditionOccurrencePointer,
                                        ConditionOccurrenceConstants.CONDITION_CONCEPT_ID),
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
                                        conditionOccurrencePointer,
                                        ConditionOccurrenceConstants.CONDITION_CONCEPT_ID),
                                    conditionOccurrenceVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(4311280)),
                            new BinaryFilterVariable(
                                new FieldVariable(
                                    new FieldPointer(
                                        conditionAncestorPointer,
                                        ConceptAncestorConstants.ANCESTOR_CONCEPT_ID),
                                    conditionAncestorVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(4311280)))),
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(tablePointer, PersonConstants.YEAR_OF_BIRTH),
                            tableVariable),
                        BinaryFilterVariable.BinaryOperator.LESS_THAN,
                        new Literal(1983)))));
    String querySQL = query.renderSQL(context);
    assertThat(
        querySQL,
        allOf(
            //            containsString("SELECT COUNT(DISTINCT p.person_id) FROM person AS p"),
            //            containsString("JOIN condition_occurrence AS c ON c.person_id =
            // p.person_id"),
            containsString(
                "JOIN condition_ancestor AS c0 ON c0.ancestor_concept_id = c.condition_concept_id")));
    //                        containsString("WHERE ("),
    //                        containsString(
    //                            "(c.condition_concept_id = 316139 OR c0.ancestor_concept_id =
    // 316139
    // OR c.condition_concept_id = 4311280 OR c0.ancestor_concept_id = 4311280)"),
    //            containsString("AND p.year_of_birth < 1983")));
  }
}
