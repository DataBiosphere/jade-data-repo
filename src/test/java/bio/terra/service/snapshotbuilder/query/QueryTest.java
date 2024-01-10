package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import bio.terra.common.PdaoConstant;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;
import java.util.function.Function;
import javax.validation.constraints.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
public class QueryTest {

  @NotNull
  public static Query createQuery() {
    TablePointer tablePointer = TablePointer.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    return new Query(
        List.of(new FieldVariable(FieldPointer.allFields(tablePointer), tableVariable)),
        List.of(tableVariable));
  }

  @Test
  void renderSQL() {
    assertThat(createQuery().renderSQL(), is("SELECT t.* FROM table AS t"));
  }

  @Test
  void renderSqlGroupBy() {
    TablePointer tablePointer = TablePointer.fromTableName("table");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    FieldPointer fieldPointer = new FieldPointer(tablePointer, "field");
    FieldVariable fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    Query query = new Query(List.of(fieldVariable), List.of(tableVariable), List.of(fieldVariable));
    assertThat(query.renderSQL(), is("SELECT t.field FROM table AS t GROUP BY t.field"));
  }

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
    String querySQL = query.renderSQL();
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

  @Test
  void renderSQLWithDatasetModel() {
    DatasetModel dataset =
        new DatasetModel().name("name").dataProject("project").cloudPlatform(CloudPlatform.GCP);
    TablePointer conceptTablePointer =
        new TablePointer("concept", null, null, generateTableName(dataset));
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldPointer nameFieldPointer = new FieldPointer(conceptTablePointer, "concept_name");
    FieldVariable nameFieldVariable = new FieldVariable(nameFieldPointer, conceptTableVariable);
    FieldPointer idFieldPointer = new FieldPointer(conceptTablePointer, "concept_id");
    FieldVariable idFieldVariable = new FieldVariable(idFieldPointer, conceptTableVariable);

    TablePointer tablePointer =
        new TablePointer("concept_ancestor", null, null, generateTableName(dataset));
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    FieldPointer fieldPointer = new FieldPointer(tablePointer, "descendant_concept_id");
    FieldVariable fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    BinaryFilterVariable whereClause =
        new BinaryFilterVariable(
            new FieldVariable(new FieldPointer(tablePointer, "ancestor_concept_id"), tableVariable),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(100));
    Query subQuery = new Query(List.of(fieldVariable), List.of(tableVariable), whereClause);
    SubQueryFilterVariable subQueryFilterVariable =
        new SubQueryFilterVariable(idFieldVariable, SubQueryFilterVariable.Operator.IN, subQuery);
    Query query =
        new Query(
            List.of(nameFieldVariable, idFieldVariable),
            List.of(conceptTableVariable),
            subQueryFilterVariable);
    String sql = query.renderSQL();
    assertThat(
        sql,
        is(
            "SELECT c.concept_name, c.concept_id FROM `project.datarepo_name.concept` AS c "
                + "WHERE c.concept_id IN "
                + "(SELECT c.descendant_concept_id FROM `project.datarepo_name.concept_ancestor` AS c "
                + "WHERE c.ancestor_concept_id = 100)"));
  }

  public static Function<String, String> generateTableName(DatasetModel dataset) {
    return (tableName) -> {
      String dataProjectId = dataset.getDataProject();
      String bqDatasetName = PdaoConstant.PDAO_PREFIX + dataset.getName();
      return String.format("`%s.%s.%s`", dataProjectId, bqDatasetName, tableName);
    };
  }
}
