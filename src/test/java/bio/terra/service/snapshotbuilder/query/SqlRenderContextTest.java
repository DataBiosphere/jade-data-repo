package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Tag(Unit.TAG)
class SqlRenderContextTest {

  @Test
  void getTableName() {
    assertThat(new SqlRenderContext(x -> "table", null).getTableName(null), is("table"));
  }

  @Test
  void getPlatform() {
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(CloudPlatform.GCP);
    assertThat(new SqlRenderContext(x -> x, platform).getPlatform(), is(platform));
  }

  @ParameterizedTest
  @CsvSource({"concept, c", "concept_ancestor, ca", "concept__ancestor, ca", "a_b_c, abc"})
  void getDefaultAlias(String tableName, String expectedAlias) {
    assertThat(SqlRenderContext.getDefaultAlias(tableName), is(expectedAlias));
  }

  @Test
  void getAlias() {
    var context = new SqlRenderContext(x -> x, null);
    var tableVariable = SourceVariable.forPrimary(TablePointer.fromTableName("table"));
    assertThat("first call generates a new alias", context.getAlias(tableVariable), is("t"));
    assertThat(
        "second call should find the alias in the cache", context.getAlias(tableVariable), is("t"));
    var tableVariable2 = SourceVariable.forPrimary(TablePointer.fromTableName("table2"));
    assertThat(
        "table with the same prefix generates a new alias",
        context.getAlias(tableVariable2),
        is("t1"));
  }
}
