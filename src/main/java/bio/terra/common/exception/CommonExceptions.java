package bio.terra.common.exception;

import java.util.function.Supplier;

/** Common exceptions that tend to be thrown in different areas */
public class CommonExceptions {

  public static final FeatureNotImplementedException TRANSACTIONS_NOT_IMPLEMENTED_IN_AZURE =
      new FeatureNotImplementedException(
          "Transaction support is not yet implemented for Azure backed datasets");

  public static final FeatureNotImplementedException AZURE_NOT_SUPPORTED =
      new FeatureNotImplementedException("Azure is no longer supported by TDR");

  public static <T> Supplier<T> azureNotSupported() {
    return () -> {
      throw AZURE_NOT_SUPPORTED;
    };
  }
}
