package bio.terra.tanagra.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.QueryTest;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class SubQueryFilterVariableTest {

  @Test
  void renderSQL() {
    var subQuery = QueryTest.createQuery();

    var fieldPointer = new FieldPointer.Builder().columnName("field").build();
    var tableVariable = TableVariable.forPrimary(TablePointer.fromTableName(null, "x"));
    TableVariable.generateAliases(List.of(tableVariable));
    var fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    var filter =
        new SubQueryFilterVariable(fieldVariable, SubQueryFilterVariable.Operator.IN, subQuery);
    assertThat(filter.renderSQL(null), is("x.field IN (SELECT t.* FROM table AS t)"));
  }
}
