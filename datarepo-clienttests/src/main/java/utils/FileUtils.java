package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class FileUtils {

  private FileUtils() {}

  private static SecureRandom randomGenerator = new SecureRandom();

  /**
   * Append a random integer to the provided string.
   *
   * @param baseName the string to append to
   * @return the new string
   */
  public static String randomizeName(String baseName) {
    long suffix = randomGenerator.nextLong();
    return baseName + Long.toUnsignedString(suffix);
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

  /**
   * Fetch a list of all the resources in the given directory path.
   *
   * @param resourcePath the path under the resources directory
   * @return
   */
  public static List<String> getResourcesInDirectory(String resourcePath) {
    URL resourceDirectoryURL = FileUtils.class.getClassLoader().getResource(resourcePath);
    File resourceDirectoryFile = new File(resourceDirectoryURL.getFile());
    String[] resourceFileNames = resourceDirectoryFile.list();
    return resourceFileNames == null ? new ArrayList<>() : Arrays.asList(resourceFileNames);
  }
}
