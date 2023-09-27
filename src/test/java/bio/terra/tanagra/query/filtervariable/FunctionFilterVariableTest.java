package bio.terra.tanagra.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.SqlPlatform;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class FunctionFilterVariableTest {

  @Test
  void getSubstitutionTemplate() {
    var filterVariable =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.IN,
            null,
            new Literal("value1"),
            new Literal("value2"));
    assertThat(
        filterVariable
            .getSubstitutionTemplate(SqlPlatform.BIGQUERY)
            .add("fieldVariable", "fieldVariable")
            .render(),
        is("fieldVariable IN ('value1','value2')"));
  }

  @Test
  void getFieldVariables() {
    var fieldVariable = new FieldVariable(null, null);
    var filterVariable =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.IN, fieldVariable, new Literal("value"));
    assertThat(filterVariable.getFieldVariables(), containsInAnyOrder(fieldVariable));
  }
}
