package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class SqlRenderContextTest {

  @Test
  void getTableName() {
    var context = SqlRenderContextProvider.of(CloudPlatform.GCP);
    assertThat(context.getTableName("table"), is("table"));
  }

  @Test
  void getPlatform() {
    var context = SqlRenderContextProvider.of(CloudPlatform.GCP);
    assertThat(context.getPlatform(), is(CloudPlatformWrapper.of(CloudPlatform.GCP)));
  }

  @Test
  void getDefaultAlias() {
    assertThat(SqlRenderContext.getDefaultAlias("concept"), is("c"));
    assertThat(SqlRenderContext.getDefaultAlias("concept_ancestor"), is("ca"));
    assertThat(SqlRenderContext.getDefaultAlias("concept__ancestor"), is("ca"));
    assertThat(SqlRenderContext.getDefaultAlias("a_b_c"), is("abc"));
  }

  @Test
  void getAlias() {
    var context = SqlRenderContextProvider.of(CloudPlatform.GCP);
    var tableVariable = TableVariable.forPrimary(TablePointer.fromTableName("table"));
    assertThat(context.getAlias(tableVariable), is("t"));
    assertThat(context.getAlias(tableVariable), is("t"));
    var tableVariable2 = TableVariable.forPrimary(TablePointer.fromTableName("table2"));
    assertThat(context.getAlias(tableVariable2), is("t1"));
  }
}
