package bio.terra.service.snapshotbuilder.query.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.Filter;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.SqlPlatform;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class BooleanAndOrFilterTest {

  @Test
  void buildVariable() {
    var subFilterVariable =
        new FilterVariable() {
          @Override
          public String renderSQL(SqlPlatform platform) {
            return "sql";
          }
        };
    var subFilter =
        new Filter() {
          @Override
          public FilterVariable buildVariable(
              TableVariable primaryTable, List<TableVariable> tables) {
            return subFilterVariable;
          }
        };
    var filter =
        new BooleanAndOrFilter(BooleanAndOrFilterVariable.LogicalOperator.OR, List.of(subFilter));
    var variable = filter.buildVariable(TableVariable.forPrimary(null), List.of());
    assertThat(variable.operator(), is(filter.operator()));
    assertThat(variable.subFilters().get(0), is(subFilterVariable));
  }
}
