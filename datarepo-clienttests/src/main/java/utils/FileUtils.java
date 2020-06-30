package utils;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.SecureRandom;

public final class FileUtils {

  private FileUtils() {}

  private static SecureRandom randomGenerator = new SecureRandom();

  public static String randomizeName(String baseName) {
    long suffix = randomGenerator.nextLong();
    return baseName + (suffix <= 0 ? -suffix : suffix);
  }

  /**
   * Build a stream handle to a JSON resource file.
   *
   * @throws FileNotFoundException
   */
  public static InputStream getJSONFileHandle(String resourceFilePath)
      throws FileNotFoundException {
    InputStream inputStream =
        FileUtils.class.getClassLoader().getResourceAsStream(resourceFilePath);
    if (inputStream == null) {
      throw new FileNotFoundException("Resource file not found: " + resourceFilePath);
    }
    return inputStream;
  }
}
