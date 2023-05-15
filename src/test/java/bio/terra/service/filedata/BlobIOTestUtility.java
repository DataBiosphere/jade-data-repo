package bio.terra.service.filedata;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Interface for utility classes that create cloud data for testing to implement */
public interface BlobIOTestUtility {

  long MIB = 1024 * 1024;

  /**
   * Upload file with random content
   *
   * @param blobName the name of the blobName to upload
   * @param length the length in bytes of the files to create
   * @return The passed in blob name
   */
  String uploadSourceFile(String blobName, long length);

  /**
   * Upload files with specific content
   *
   * @param blobName the base name of the blobName to upload
   * @param contents the content to upload
   * @return The final name of the blob that was created
   */
  String uploadFileWithContents(String blobName, String contents);

  /** Clear any artifacts created by this instance */
  void teardown();

  /**
   * Upload files with random content
   *
   * @param numOfFiles the number of files to upload
   * @param length the length in bytes of the files to create
   * @return A list of blob names
   */
  default List<String> uploadSourceFiles(int numOfFiles, long length) {
    return Stream.iterate(0, n -> n + 1)
        .limit(numOfFiles)
        .map(
            i ->
                uploadSourceFile(
                    String.format("%s/%s%s", i, "myTestBlob", UUID.randomUUID()), length))
        .collect(Collectors.toList());
  }

  /**
   * Create an input stream of [length] bytes
   *
   * @param length the number of bytes to create
   * @return an InputStream of random data
   */
  default InputStream createInputStream(long length) {
    return new InputStream() {
      private long dataProduced;
      private final Random rand = new Random();

      @Override
      @SuppressFBWarnings(
          value = "DMI_RANDOM_USED_ONLY_ONCE",
          justification = "this is misfire for spotbugs.  Random gets used repeatedly")
      public int read() {
        if (dataProduced == length) {
          return -1;
        }
        dataProduced++;
        return rand.nextInt(100 - 65) + 65; // starting at "A"
      }
    };
  }
}
