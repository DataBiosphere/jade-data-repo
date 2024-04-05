package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.CloudPlatform;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class QueryTestUtils {
  public static TablePointer fromTableName(String tableName) {
    return TablePointer.fromTableName(tableName);
  }

  public static SqlRenderContext createContext(CloudPlatform cloudPlatform) {
    return new SqlRenderContext(x -> x, CloudPlatformWrapper.of(cloudPlatform)) {
      @Override
      public String toString() {
        return cloudPlatform.toString();
      }
    };
  }

  public static class Contexts implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Arrays.stream(CloudPlatform.values())
          .map(QueryTestUtils::createContext)
          .map(Arguments::of);
    }
  }
}
