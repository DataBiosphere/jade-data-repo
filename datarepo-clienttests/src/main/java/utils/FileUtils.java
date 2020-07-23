package utils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public final class FileUtils {
  private static ArrayList<String> createdScratchFiles;

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
   * @return the new file handle
   * @throws FileNotFoundException if the resource file doesn't exist
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
   * @return the list of resource files
   */
  public static List<String> getResourcesInDirectory(String resourcePath) {
    URL resourceDirectoryURL = FileUtils.class.getClassLoader().getResource(resourcePath);
    File resourceDirectoryFile = new File(resourceDirectoryURL.getFile());
    String[] resourceFileNames = resourceDirectoryFile.list();
    return resourceFileNames == null ? new ArrayList<>() : Arrays.asList(resourceFileNames);
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
    File localFile = createNewFile(localFileName);

    ReadableByteChannel readChannel = Channels.newChannel(url.openStream());
    FileOutputStream outputStream = new FileOutputStream(localFile);
    outputStream.getChannel().transferFrom(readChannel, 0, Long.MAX_VALUE);
    return localFile;
  }

  /**
   * Create a new file. Delete the existing one first, if applicable. The file is created relative
   * to the current directory.
   *
   * @param fileName the name of the file to create
   * @return the new file
   */
  public static File createNewFile(String fileName) throws IOException {
    File newFile = new File(fileName);
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
   * Fetch the gsPath path.
   *
   * <p>// take the file name (e.g. testRetrieveDRSSnapshot.json) and byte array // build the
   * blobinfo and create the storage object // save a reference to the blobid (or whatever you need
   * to delete the storage object) in some static list in that class
   *
   * @param fileId the if for the fileModel
   * @param storage the storage
   * @param testConfigGetIngestbucket the bucket
   * @return the gsPath
   */
  public static String getFileRefs(
      String fileId, Storage storage, String testConfigGetIngestbucket) {
    createdScratchFiles = new ArrayList<>();
    // generate a JSON file with the fileref
    String jsonLine =
        "{\"VCF_File_Name\":\"name1\", \"Description\":\"description1\", \"VCF_File_Ref\":\""
            + fileId
            + "\"}\n";
    // load a JSON file that contains the table rows to load into the test bucket
    String jsonFileName = "this-better-pass.json";
    String dirInCloud = "scratch/testRetrieveSnapshot/" + UUID.randomUUID().toString();
    BlobInfo ingestTableBlob =
        BlobInfo.newBuilder(testConfigGetIngestbucket, dirInCloud + "/" + jsonFileName).build();

    storage.create(ingestTableBlob, jsonLine.getBytes(StandardCharsets.UTF_8));

    // save a reference to the JSON file so we can delete it in cleanup()
    createdScratchFiles.add(dirInCloud + "/" + jsonFileName);
    String gsPath = "gs://" + testConfigGetIngestbucket + "/" + dirInCloud + "/" + jsonFileName;

    return gsPath;
  }

  public static void cleanupScratchFiles(Storage storage, String testConfigGetIngestbucket) {
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
