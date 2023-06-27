package bio.terra.service.resourcemanagement;

import bio.terra.app.model.AzureRegion;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.model.BillingProfileModel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Primary
@Component
public class AzureDataLocationSelector {

  public String createStorageAccountName(
      String prefix,
      AzureRegion region,
      BillingProfileModel billingProfile,
      boolean isSecureMonitoringEnabled) {
    int maxStorageAccountNameLength = 24;
    int randomLength = maxStorageAccountNameLength - prefix.length();
    String seed = region.getValue() + billingProfile;
    if (isSecureMonitoringEnabled) {
      seed += " secure";
    }
    return prefix + armUniqueString(seed, randomLength);
  }

  /**
   * Generate an n-character unique string in a way that mimics what azure's ARM template function
   * uses. Note: this method is deterministic (e.g. calling with the same seed value will provide
   * the same result)
   *
   * @param seed The value to generate a unique string for
   * @param size The number of characters to generate
   * @return An n-character unique string based on what was passed into seed
   */
  public static String armUniqueString(String seed, int size) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-512");
      byte[] digest = md.digest(seed.getBytes(StandardCharsets.UTF_8));
      StringBuilder result = new StringBuilder();
      if (size <= 0) {
        throw new IllegalArgumentException(
            "The size of the requested unique string must be greater than 0");
      }
      if (digest.length < size) {
        throw new IllegalArgumentException(
            String.format(
                "The size of the requested unique string is too long (%s < %s)",
                digest.length, size));
      }
      for (int i = 0; i < size; i++) {
        int b = Byte.toUnsignedInt(digest[i]);
        char c = (char) ((b % 26) + (byte) 'a');
        result.append(c);
      }
      return result.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new FeatureNotImplementedException("SHA512 not supported in this JVM", e);
    }
  }
}
