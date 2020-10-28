package common.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

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
   * Build a stream handle to a resource file.
   *
   * @return the new file handle
   * @throws FileNotFoundException if the resource file doesn't exist
   */
  public static InputStream getResourceFileHandle(String resourceFilePath)
      throws FileNotFoundException {
    InputStream inputStream =
        FileUtils.class.getClassLoader().getResourceAsStream(resourceFilePath);
    if (inputStream == null) {
      throw new FileNotFoundException("Resource file not found: " + resourceFilePath);
    }
    return inputStream;
  }

  /**
   * Fetch a list of all the files in the given directory path.
   *
   * @param directoryFile the directory to search
   * @return the list of files in the directory
   */
  public static List<String> getFilesInDirectory(File directoryFile) throws IOException {
    // recursively fetch all files under the given directory
    List<Path> filePaths =
        Files.walk(directoryFile.toPath())
            .filter(Files::isRegularFile)
            .collect(Collectors.toList());

    // convert each file to a file path relative to the specified directory
    List<String> fileNames = new ArrayList<>();
    for (Path filePath : filePaths) {
      String relativeFileName =
          filePath.toString().replace(directoryFile.getPath(), directoryFile.getName());
      fileNames.add(relativeFileName);
    }
    return fileNames;
  }

  /**
   * Create a copy of a local or remote file, given its URL. For local files, use the "file://"
   * protocol. The copy is created relative to the current directory.
   *
   * @param url the local or remote url
   * @param localFileName the name of the copy to create
   * @return the new file
   */
  public static File createCopyOfFileFromURL(URL url, String localFileName) throws IOException {
    File localFile = createNewFile(new File(localFileName));

    ReadableByteChannel readChannel = Channels.newChannel(url.openStream());
    FileOutputStream outputStream = new FileOutputStream(localFile);
    outputStream.getChannel().transferFrom(readChannel, 0, Long.MAX_VALUE);
    return localFile;
  }

  /**
   * Create a new file. Delete the existing one first, if applicable. The file is created relative
   * to the current directory.
   *
   * @param newFile the file to create
   * @return the new file
   */
  public static File createNewFile(File newFile) throws IOException {
    if (newFile.exists()) {
      boolean deleteSucceeded = newFile.delete();
      if (!deleteSucceeded) {
        throw new RuntimeException("Deleting existing file failed: " + newFile.getAbsolutePath());
      }
    }
    boolean createSucceeded = newFile.createNewFile();
    if (!createSucceeded || !newFile.exists()) {
      throw new RuntimeException("Creating new file failed: " + newFile.getAbsolutePath());
    }
    return newFile;
  }

  /**
   * Compress a directory (tar.gz) and write the archive file to the give path.
   *
   * @param directoryToCompress the top-level directory to compress
   * @param archiveFile the path to write the archive file to
   */
  public static void compressDirectory(Path directoryToCompress, Path archiveFile)
      throws IOException {
    // try with resources to make sure the streams get closed
    try (OutputStream fileOS = Files.newOutputStream(archiveFile);
        OutputStream gzipOS = new GzipCompressorOutputStream(fileOS);
        ArchiveOutputStream tarOS = new TarArchiveOutputStream(gzipOS)) {

      Files.walkFileTree(
          directoryToCompress,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path fileToArchive, BasicFileAttributes attrs)
                throws IOException {
              // archive entry name should be the path relative to the top-level directory being
              // archived
              String entryName = directoryToCompress.relativize(fileToArchive).toString();

              // create a new archive entry, copy over the file contents
              ArchiveEntry entry = tarOS.createArchiveEntry(fileToArchive.toFile(), entryName);
              tarOS.putArchiveEntry(entry);
              try (InputStream fileIS = Files.newInputStream(fileToArchive)) {
                IOUtils.copy(fileIS, tarOS);
              }
              tarOS.closeArchiveEntry();

              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path fileToArchive, IOException exc) {
              throw new RuntimeException(
                  "Error walking file tree inside directory to compress: "
                      + directoryToCompress.toAbsolutePath().toString());
            }
          });

      tarOS.finish();
    }
  }

  /**
   * Read a JSON-formatted file into a Java object using the Jackson object mapper.
   *
   * @param directory the directory where the file is
   * @param fileName the file name
   * @param javaObjectClass the Java object class
   * @param <T> the Java object class to map the file contents to
   * @return an instance of the Java object class
   */
  public static <T> T readOutputFileIntoJavaObject(
      Path directory, String fileName, Class<T> javaObjectClass) throws Exception {
    // get a reference to the file
    File outputFile = directory.resolve(fileName).toFile();
    if (!outputFile.exists()) {
      return null;
    }

    // use Jackson to map the file contents to the TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();
    try (FileInputStream inputStream = new FileInputStream(outputFile)) {
      return objectMapper.readValue(inputStream, javaObjectClass);
    }
  }
}
