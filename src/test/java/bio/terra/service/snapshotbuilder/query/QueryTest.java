package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.snapshotbuilder.query.exceptions.InvalidRenderSqlParameter;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
  @EnumSource(CloudPlatform.class)
  void renderSQL(CloudPlatform platform) {
    assertThat(
        createQuery().renderSQL(CloudPlatformWrapper.of(platform)),
        is("SELECT t.* FROM table AS t"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSQLWithLimit(CloudPlatform platform) {
    CloudPlatformWrapper cloudPlatformWrapper = CloudPlatformWrapper.of(platform);
    String actual = createQueryWithLimit().renderSQL(CloudPlatformWrapper.of(platform));
    String expected = "SELECT t.* FROM table AS t";
    if (cloudPlatformWrapper.isAzure()) {
      assertThat(actual, is("TOP 25 " + expected));
    } else if (cloudPlatformWrapper.isGcp()) {
      assertThat(actual, is(expected + " LIMIT 25"));
    }
  }

  @Test
  void renderSQLWithLimitNullWrapper() {
    assertThrows(InvalidRenderSqlParameter.class, () -> createQueryWithLimit().renderSQL(null));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderSqlGroupBy(CloudPlatform platform) {
    TablePointer tablePointer = QueryTestUtils.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    FieldPointer fieldPointer = new FieldPointer(tablePointer, "field");
    FieldVariable fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    Query query = new Query(List.of(fieldVariable), List.of(tableVariable), List.of(fieldVariable));
    assertThat(
        query.renderSQL(CloudPlatformWrapper.of(platform)),
        is("SELECT t.field FROM table AS t GROUP BY t.field"));
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void renderComplexSQL(CloudPlatform platform) {
    TablePointer tablePointer = QueryTestUtils.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    TablePointer conditionOccurrencePointer = QueryTestUtils.fromTableName("condition_occurrence");
    TableVariable conditionOccurrenceVariable =
        TableVariable.forJoined(
            conditionOccurrencePointer,
            "person_id",
            new FieldVariable(new FieldPointer(tablePointer, "person_id"), tableVariable));

    TablePointer conditionAncestorPointer = QueryTestUtils.fromTableName("condition_ancestor");
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
    String querySQL = query.renderSQL(CloudPlatformWrapper.of(platform));
    assertThat(
        querySQL,
        allOf(
            containsString("SELECT COUNT(DISTINCT p.person_id) FROM person AS p"),
            containsString("JOIN condition_occurrence AS c ON c.person_id = p.person_id"),
            containsString(
                "JOIN condition_ancestor AS c0 ON c0.ancestor_concept_id = c.condition_concept_id"),
            containsString("WHERE ("),
            containsString(
                "(c.condition_concept_id = 316139 OR c0.ancestor_concept_id = 316139 OR c.condition_concept_id = 4311280 OR c0.ancestor_concept_id = 4311280)"),
            containsString("AND p.year_of_birth < 1983")));
  }
}
