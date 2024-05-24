package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.tables.Table;
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
    return new Query.Builder()
        .select(
            List.of(
                new FieldVariable(
                    FieldPointer.allFields(tableVariable.getTablePointer()), tableVariable)))
        .tables(List.of(new Table(tableVariable)))
        .build();
  }

  @NotNull
  public static Query createQueryWithLimit() {
    TableVariable tableVariable = makeTableVariable();
    return new Query.Builder()
        .select(
            List.of(
                new FieldVariable(
                    FieldPointer.allFields(tableVariable.getTablePointer()), tableVariable)))
        .tables(List.of(new Table(tableVariable)))
        .where(null)
        .limit(25)
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
        new Query.Builder()
            .select(List.of(fieldVariable))
            .tables(List.of(new Table(tableVariable)))
            .groupBy(List.of(fieldVariable))
            .build();
    assertThat(query.renderSQL(context), is("SELECT t.field FROM table AS t GROUP BY t.field"));
  }
}
