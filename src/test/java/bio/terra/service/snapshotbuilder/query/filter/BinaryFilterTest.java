package bio.terra.service.snapshotbuilder.query.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BinaryFilterTest {

  @Test
  void buildVariable() {
    var fieldPointer = new FieldPointer(null, "column");
    var filter =
        new BinaryFilter(
            fieldPointer, BinaryFilterVariable.BinaryOperator.EQUALS, new Literal("value"));
    var variable = filter.buildVariable(SourceVariable.forPrimary(null), List.of());
    assertThat(variable.operator(), is(filter.operator()));
    assertThat(variable.value(), is(filter.value()));
    assertThat(variable.fieldVariable().getAliasOrColumnName(), is(fieldPointer.getColumnName()));
  }
}
