package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptAncestor;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
public class QueryTest {

  @NotNull
  public static Query createQuery() {
    SourceVariable sourceVariable = makeTableVariable();
    return new Query(
        List.of(
            new FieldVariable(
                FieldPointer.allFields(sourceVariable.getSourcePointer()), sourceVariable)),
        List.of(sourceVariable));
  }

  @NotNull
  public static Query createQueryWithLimit() {
    SourceVariable sourceVariable = makeTableVariable();
    return new Query(
        List.of(
            new FieldVariable(
                FieldPointer.allFields(sourceVariable.getSourcePointer()), sourceVariable)),
        List.of(sourceVariable),
        null,
        25);
  }

  private static SourceVariable makeTableVariable() {
    TablePointer tablePointer = TablePointer.fromTableName("table");
    return SourceVariable.forPrimary(tablePointer);
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQL(SqlRenderContext context) {
    assertThat(createQuery().renderSQL(context), is("SELECT t.* FROM table AS t"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQLWithLimit(SqlRenderContext context) {
    String query = "t.* FROM table AS t";
    String expected =
        context.getPlatform().choose("SELECT " + query + " LIMIT 25", "SELECT TOP 25 " + query);
    assertThat(createQueryWithLimit().renderSQL(context), is(expected));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSqlGroupBy(SqlRenderContext context) {
    TablePointer tablePointer = TablePointer.fromTableName("table");
    SourceVariable sourceVariable = SourceVariable.forPrimary(tablePointer);
    FieldPointer fieldPointer = new FieldPointer(tablePointer, "field");
    FieldVariable fieldVariable = new FieldVariable(fieldPointer, sourceVariable);
    Query query =
        new Query(List.of(fieldVariable), List.of(sourceVariable), List.of(fieldVariable));
    assertThat(query.renderSQL(context), is("SELECT t.field FROM table AS t GROUP BY t.field"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderComplexSQL(SqlRenderContext context) {
    TablePointer tablePointer = TablePointer.fromTableName(Person.TABLE_NAME);
    SourceVariable sourceVariable = SourceVariable.forPrimary(tablePointer);

    TablePointer conditionOccurrencePointer =
        TablePointer.fromTableName(ConditionOccurrence.TABLE_NAME);
    SourceVariable conditionOccurrenceVariable =
        SourceVariable.forJoined(
            conditionOccurrencePointer,
            Person.PERSON_ID,
            new FieldVariable(new FieldPointer(tablePointer, Person.PERSON_ID), sourceVariable));

    TablePointer conditionAncestorPointer = TablePointer.fromTableName("condition_ancestor");
    SourceVariable conditionAncestorVariable =
        SourceVariable.forJoined(
            conditionAncestorPointer,
            ConceptAncestor.ANCESTOR_CONCEPT_ID,
            new FieldVariable(
                new FieldPointer(
                    conditionOccurrencePointer, ConditionOccurrence.CONDITION_CONCEPT_ID),
                conditionOccurrenceVariable));

    Query query =
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(tablePointer, Person.PERSON_ID, "COUNT"),
                    sourceVariable,
                    null,
                    true)),
            List.of(sourceVariable, conditionOccurrenceVariable, conditionAncestorVariable),
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
                                        ConditionOccurrence.CONDITION_CONCEPT_ID),
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
                                        ConditionOccurrence.CONDITION_CONCEPT_ID),
                                    conditionOccurrenceVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(4311280)),
                            new BinaryFilterVariable(
                                new FieldVariable(
                                    new FieldPointer(
                                        conditionAncestorPointer,
                                        ConceptAncestor.ANCESTOR_CONCEPT_ID),
                                    conditionAncestorVariable),
                                BinaryFilterVariable.BinaryOperator.EQUALS,
                                new Literal(4311280)))),
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(tablePointer, Person.YEAR_OF_BIRTH), sourceVariable),
                        BinaryFilterVariable.BinaryOperator.LESS_THAN,
                        new Literal(1983)))));
    String querySQL = query.renderSQL(context);
    assertThat(
        querySQL,
        allOf(
            containsString("SELECT COUNT(DISTINCT p.person_id) FROM person AS p"),
            containsString("JOIN condition_occurrence AS co ON co.person_id = p.person_id"),
            containsString(
                "JOIN condition_ancestor AS ca ON ca.ancestor_concept_id = co.condition_concept_id"),
            containsString("WHERE ("),
            containsString(
                "(co.condition_concept_id = 316139 OR ca.ancestor_concept_id = 316139 OR co.condition_concept_id = 4311280 OR ca.ancestor_concept_id = 4311280)"),
            containsString("AND p.year_of_birth < 1983")));
  }
}
