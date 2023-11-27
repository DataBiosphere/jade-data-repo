package bio.terra.service.snapshotbuilder.query.filtervariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class FunctionFilterVariableTest {

  @Test
  void renderSQL() {
    TableVariable table = TableVariable.forPrimary(TablePointer.fromTableName("table"));
    TableVariable.generateAliases(List.of(table));
    var filterVariable =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.IN,
            new FieldVariable(new FieldPointer(null, "column"), table),
            new Literal("value1"),
            new Literal("value2"));
    assertThat(filterVariable.renderSQL(), is("t.column IN ('value1','value2')"));
  }
}