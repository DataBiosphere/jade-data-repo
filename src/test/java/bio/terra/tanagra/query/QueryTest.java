package bio.terra.tanagra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.List;
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
    assertThat(createQuery().renderSQL(null), is("SELECT t.* FROM table AS t"));
  }
}
