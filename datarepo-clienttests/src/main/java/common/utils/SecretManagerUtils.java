package common.utils;

import com.google.cloud.secretmanager.v1.*;
import java.io.*;
import org.slf4j.*;
import runner.config.*;

public class SecretManagerUtils {
  public static final String secretSuffix = "-testrunner-inuse";
  private static final Logger logger = LoggerFactory.getLogger(SecretManagerUtils.class);

  private static SecretManagerServiceClient secretManagerServiceClient;

  private SecretManagerUtils() {}

  private static void buildSecretManagerServiceClient() throws IOException {
    if (secretManagerServiceClient == null) {
      secretManagerServiceClient = SecretManagerServiceClient.create();
    }
  }

  private static void closeSecretManagerServiceClient() throws IOException {
    if (secretManagerServiceClient != null) {
      secretManagerServiceClient.close();
    }
  }

  public static void lockNamespace(ServerSpecification server) throws Exception {
    buildSecretManagerServiceClient();
    String secretNameString = server.namespace + secretSuffix;
    SecretName secretName = SecretName.of(server.project, secretNameString);

    if (secretExists(secretName)) {
      throw new Exception(
          "LockNamespace FAILED: Namepsace " + server.namespace + " already in use.");
    }
    createSecret(server, secretNameString);
    closeSecretManagerServiceClient();
  }

  public static void unlockNamespace(ServerSpecification server) throws IOException {
    buildSecretManagerServiceClient();
    String secretName = server.namespace + secretSuffix;
    deleteSecret(secretName);
    closeSecretManagerServiceClient();
  }

  private static void createSecret(ServerSpecification server, String secretName) {
    ProjectName projectName = ProjectName.of(server.project);
    Secret secret =
        Secret.newBuilder()
            .setReplication(
                Replication.newBuilder()
                    .setAutomatic(Replication.Automatic.newBuilder().build())
                    .build())
            .build();

    // Create the secret.
    Secret createdSecret = secretManagerServiceClient.createSecret(projectName, secretName, secret);
    System.out.printf("Created secret %s\n", createdSecret.getName());
  }

  private static boolean secretExists(SecretName secretName) {
    try {
      secretManagerServiceClient.getSecret(secretName);
      return true;
    } catch (Exception ex) {
      logger.debug("Secret not found.");
    }
    return false;
  }

  private static void deleteSecret(String secretName) {
    secretManagerServiceClient.deleteSecret(secretName);
  }
}
