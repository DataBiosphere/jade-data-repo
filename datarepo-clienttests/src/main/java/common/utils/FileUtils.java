package common.utils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class FileUtils {
  private static ArrayList<String> createdScratchFiles;

  private FileUtils() {}

  private static Storage storage;
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
   * Create the gs path for scratch file.
   *
   * <p>// take the file name (e.g. testRetrieveDRSSnapshot.json) and byte array // build the
   * blobinfo and create the storage object // save a reference to the file to delete it later
   *
   * @param fileRefBytes the substance of the scratch file
   * @param fileRefName the name for the scratch file
   * @param testConfigGetIngestbucket the gc bucket where the scratch files are
   * @return the gsPath
   */
  public static String createGsPath(
      byte[] fileRefBytes, String fileRefName, String testConfigGetIngestbucket) {
    createdScratchFiles = new ArrayList<>();
    storage = StorageOptions.getDefaultInstance().getService();

    // load a JSON file that contains the table rows to load into the test bucket
    BlobInfo ingestTableBlob = BlobInfo.newBuilder(testConfigGetIngestbucket, fileRefName).build();

    storage.create(ingestTableBlob, fileRefBytes);

    // save a reference to the JSON file so we can delete it in cleanup()
    createdScratchFiles.add(fileRefName);
    return String.format("gs://%s/%s", testConfigGetIngestbucket, fileRefName);
  }

  public static void cleanupScratchFiles(String testConfigGetIngestbucket) {
    storage = StorageOptions.getDefaultInstance().getService();
    // do the delete loop you've already coded.
    // delete scratch files -- This should be pulled into the test runner?
    for (String path : createdScratchFiles) {
      Blob scratchBlob = storage.get(BlobId.of(testConfigGetIngestbucket, path));
      if (scratchBlob != null) {
        scratchBlob.delete();
      }
    }
  }
}
