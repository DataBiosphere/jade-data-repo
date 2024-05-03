package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.CloudPlatform;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class SqlRenderContextProvider implements ArgumentsProvider {
  public static SqlRenderContext of(CloudPlatform cloudPlatform) {
    return new SqlRenderContext(x -> x, CloudPlatformWrapper.of(cloudPlatform)) {
      @Override
      public String toString() {
        // Overridden to improve the display of a context in the test run output.
        return cloudPlatform.toString();
      }
    };
  }

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
    return Arrays.stream(CloudPlatform.values())
        .map(SqlRenderContextProvider::of)
        .map(Arguments::of);
  }
}
