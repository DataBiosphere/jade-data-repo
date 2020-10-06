package common.utils;

import runner.config.*;

public class TestConfigurationUtils {

  public static final String serverFileEnvironmentVarName = "TEST_RUNNER_SERVER_SPECIFICATION_FILE";

  public static String readServerEnvironmentVariable() {
    // the server specification is determined by the following, in order:
    //   1. environment variable
    //   2. test suite server property
    //   3. test configuration server property
    String serverFileEnvironmentVarValue = System.getenv(serverFileEnvironmentVarName);
    return serverFileEnvironmentVarValue;
  }

  public static String getNamespace() throws Exception {
    // read in the server file
    String serverEnvVar = readServerEnvironmentVariable();
    if (serverEnvVar == null) {
      throw new Exception(serverFileEnvironmentVarName + " env variable must be defined");
    }
    return ServerSpecification.fromJSONFile(serverEnvVar).namespace;
  }
}
