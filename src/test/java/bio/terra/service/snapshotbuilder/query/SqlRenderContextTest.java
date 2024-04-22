package bio.terra.service.snapshotbuilder.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

@Tag(Unit.TAG)
public class SqlRenderContextTest {

  public static SqlRenderContext createContext(CloudPlatform cloudPlatform) {
    return new SqlRenderContext(x -> x, CloudPlatformWrapper.of(cloudPlatform)) {
      @Override
      public String toString() {
        // Overridden to improve the display of a context in the test run output.
        return cloudPlatform.toString();
      }
    };
  }

  public static class Contexts implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Arrays.stream(CloudPlatform.values())
          .map(SqlRenderContextTest::createContext)
          .map(Arguments::of);
    }
  }

  @Test
  void getTableName() {
    var context = createContext(CloudPlatform.GCP);
    assertThat(context.getTableName("table"), is("table"));
  }

  @Test
  void getPlatform() {
    var context = createContext(CloudPlatform.GCP);
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
    var context = createContext(CloudPlatform.GCP);
    var tableVariable = TableVariable.forPrimary(TablePointer.fromTableName("table"));
    assertThat(context.getAlias(tableVariable), is("t"));
    assertThat(context.getAlias(tableVariable), is("t"));
    var tableVariable2 = TableVariable.forPrimary(TablePointer.fromTableName("table2"));
    assertThat(context.getAlias(tableVariable2), is("t1"));
  }
}
