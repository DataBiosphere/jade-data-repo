package bio.terra.tanagra.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BinaryFilterVariableTest {

  @Test
  void getSubstitutionTemplate() {
    var binaryOperator = BinaryFilterVariable.BinaryOperator.EQUALS;
    var literal = new Literal("foo");
    var filterVariable = new BinaryFilterVariable(null, binaryOperator, literal);
    assertThat(filterVariable.getSubstitutionTemplate(null), is("${fieldVariable} = 'foo'"));
  }

  @Test
  void getFieldVariables() {
    var fieldVariable = new FieldVariable(null, null);
    var filterVariable = new BinaryFilterVariable(fieldVariable, null, null);
    assertThat(filterVariable.getFieldVariables(), is(List.of(fieldVariable)));
  }
}
