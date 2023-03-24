package bio.terra.tanagra.utils;

import bio.terra.tanagra.exception.SystemException;
import com.google.common.io.CharStreams;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for manipulating files on disk. */
public final class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {}

  /**
   * Build a stream to a resource file.
   *
   * @return the new file stream
   * @throws RuntimeException if the resource file doesn't exist
   */
  public static InputStream getResourceFileStream(Path resourceFilePath) {
    InputStream inputStream =
        FileUtils.class.getClassLoader().getResourceAsStream(resourceFilePath.toString());
    if (inputStream == null) {
      throw new SystemException("Resource file not found: " + resourceFilePath);
    }
    return inputStream;
  }

  /**
   * Build a stream to a file on disk.
   *
   * @return the new file stream
   * @throws RuntimeException if the file doesn't exist
   */
  public static InputStream getFileStream(Path filePath) {
    try {
      return Files.newInputStream(Path.of(filePath.toAbsolutePath().toString()));
    } catch (IOException ioEx) {
      throw new SystemException("Error opening file stream: " + filePath, ioEx);
    }
  }

  /** Create the file and any parent directories if they don't already exist. */
  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED",
      justification =
          "A file not found exception will be thrown anyway in the calling method if the mkdirs or createNewFile calls fail.")
  public static void createFile(Path path) throws IOException {
    File file = path.toFile();
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }
  }

  /** Create the directory and any parent directories if they don't already exist. */
  public static void createDirectoryIfNonexistent(Path path) {
    if (!path.toFile().exists()) {
      boolean mkdirsSuccess = path.toFile().mkdirs();
      if (!mkdirsSuccess) {
        throw new SystemException("mkdirs failed for directory: " + path.toAbsolutePath());
      }
    }
  }

  /**
   * Read a file into a string.
   *
   * @param inputStream the stream to the file contents
   * @return a Java String representing the file contents
   */
  public static String readStringFromFile(InputStream inputStream) {
    try {
      return CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
          .replace(System.lineSeparator(), " ");
    } catch (IOException ioEx) {
      throw new SystemException("Error reading file contents", ioEx);
    }
  }

  /**
   * Write a string directly to a file.
   *
   * @param path the file path to write to
   * @param fileContents the string to write
   * @return the file path that was written to
   */
  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED",
      justification =
          "A file not found exception will be thrown anyway in this same method if the mkdirs or createNewFile calls fail.")
  public static Path writeStringToFile(Path path, String fileContents) throws IOException {
    LOGGER.debug("Writing to file: {}", path);

    // create the file and any parent directories if they don't already exist
    createFile(path);

    return Files.write(path, fileContents.getBytes(StandardCharsets.UTF_8));
  }
}
