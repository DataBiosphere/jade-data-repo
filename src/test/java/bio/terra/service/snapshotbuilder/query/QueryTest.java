package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
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
    TableVariable tableVariable = makeTableVariable();
    return new QueryBuilder()
        .addSelect(
            List.of(
                new FieldVariable(
                    FieldPointer.allFields(tableVariable.getTablePointer()), tableVariable)))
        .addTables(List.of(tableVariable))
        .build();
  }

  @NotNull
  public static Query createQueryWithLimit() {
    TableVariable tableVariable = makeTableVariable();
    return new QueryBuilder()
        .addSelect(
            List.of(
                new FieldVariable(
                    FieldPointer.allFields(tableVariable.getTablePointer()), tableVariable)))
        .addTables(List.of(tableVariable))
        .addWhere(null)
        .addLimit(25)
        .build();
  }

  private static TableVariable makeTableVariable() {
    TablePointer tablePointer = TablePointer.fromTableName("table");
    return TableVariable.forPrimary(tablePointer);
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
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    FieldPointer fieldPointer = new FieldPointer(tablePointer, "field");
    FieldVariable fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    Query query =
        new QueryBuilder()
            .addSelect(List.of(fieldVariable))
            .addTables(List.of(tableVariable))
            .addGroupBy(List.of(fieldVariable))
            .build();
    assertThat(query.renderSQL(context), is("SELECT t.field FROM table AS t GROUP BY t.field"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderComplexSQL(SqlRenderContext context) {
    TablePointer tablePointer = TablePointer.fromTableName(Person.TABLE_NAME);
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    TablePointer conditionOccurrencePointer =
        TablePointer.fromTableName(ConditionOccurrence.TABLE_NAME);
    TableVariable conditionOccurrenceVariable =
        TableVariable.forJoined(
            conditionOccurrencePointer,
            Person.PERSON_ID,
            new FieldVariable(new FieldPointer(tablePointer, Person.PERSON_ID), tableVariable));

    TablePointer conditionAncestorPointer = TablePointer.fromTableName("condition_ancestor");
    TableVariable conditionAncestorVariable =
        TableVariable.forJoined(
            conditionAncestorPointer,
            ConceptAncestor.ANCESTOR_CONCEPT_ID,
            new FieldVariable(
                new FieldPointer(
                    conditionOccurrencePointer, ConditionOccurrence.CONDITION_CONCEPT_ID),
                conditionOccurrenceVariable));

    Query query =
        new QueryBuilder()
            .addSelect(
                List.of(
                    new FieldVariable(
                        new FieldPointer(tablePointer, Person.PERSON_ID, "COUNT"),
                        tableVariable,
                        null,
                        true)))
            .addTables(
                List.of(tableVariable, conditionOccurrenceVariable, conditionAncestorVariable))
            .addWhere(
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
                                new FieldPointer(tablePointer, Person.YEAR_OF_BIRTH),
                                tableVariable),
                            BinaryFilterVariable.BinaryOperator.LESS_THAN,
                            new Literal(1983)))))
            .build();
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
