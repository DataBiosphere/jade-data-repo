package bio.terra.common.exception;

/** Common exceptions that tend to be thrown in different areas */
public class CommonExceptions {

  public static final FeatureNotImplementedException TRANSACTIONS_NOT_IMPLEMENTED_IN_AZURE =
      new FeatureNotImplementedException(
          "Transaction support is not yet implemented for Azure backed datasets");
}
