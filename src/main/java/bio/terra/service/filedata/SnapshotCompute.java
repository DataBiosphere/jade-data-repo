package bio.terra.service.filedata;

import bio.terra.service.filedata.exception.DirectoryMetadataComputeException;
import bio.terra.service.filedata.exception.FileNotFoundException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class SnapshotCompute {

  // Recursively compute the size and checksums of a directory
  public static FireStoreDirectoryEntry computeDirectory(
      SnapshotComputeHelper computeHelper,
      FireStoreDirectoryEntry dirEntry,
      List<FireStoreDirectoryEntry> updateBatch)
      throws InterruptedException {

    String fullPath = SnapshotCompute.getFullPath(dirEntry.getPath(), dirEntry.getName());
    List<FireStoreDirectoryEntry> enumDir = computeHelper.enumerateDirectory(fullPath);

    List<FireStoreDirectoryEntry> enumComputed = new ArrayList<>();

    // Recurse to compute results from underlying directories
    try (Stream<FireStoreDirectoryEntry> stream = enumDir.stream()) {
      enumComputed.addAll(
          stream
              .filter(f -> !f.getIsFileRef())
              .map(
                  f -> {
                    try {
                      return computeDirectory(computeHelper, f, updateBatch);
                    } catch (InterruptedException e) {
                      throw new DirectoryMetadataComputeException(
                          "Error computing directory metadata", e);
                    }
                  })
              .collect(Collectors.toList()));
    }

    // Collect metadata for file objects in the directory
    SnapshotCompute.collectDirectoryContentMetadata(enumDir, enumComputed, computeHelper);

    // Compute this directory's checksums and size
    SnapshotCompute.updateDirectoryEntryChecksums(dirEntry, enumComputed);
    computeHelper.updateEntry(dirEntry, updateBatch);

    return dirEntry;
  }

  public static void collectDirectoryContentMetadata(
      List<FireStoreDirectoryEntry> enumDir,
      List<FireStoreDirectoryEntry> enumComputed,
      SnapshotComputeHelper computeHelper)
      throws InterruptedException {
    // Collect metadata for file objects in the directory
    try (Stream<FireStoreDirectoryEntry> stream = enumDir.stream()) {
      // Group FireStoreDirectoryEntry objects by dataset Id to process one dataset at a time
      final Map<String, List<FireStoreDirectoryEntry>> fileRefsByDatasetId =
          stream
              .filter(FireStoreDirectoryEntry::getIsFileRef)
              .collect(Collectors.groupingBy(FireStoreDirectoryEntry::getDatasetId));

      for (Map.Entry<String, List<FireStoreDirectoryEntry>> entry :
          fileRefsByDatasetId.entrySet()) {
        // Retrieve the file metadata from Firestore
        final List<FireStoreFile> fireStoreFiles = computeHelper.batchRetrieveFileMetadata(entry);

        final AtomicInteger index = new AtomicInteger(0);
        enumComputed.addAll(
            CollectionUtils.collect(
                entry.getValue(),
                dirItem -> {
                  final FireStoreFile file = fireStoreFiles.get(index.getAndIncrement());
                  if (file == null) {
                    throw new FileNotFoundException("File metadata was missing");
                  }
                  return dirItem
                      .size(file.getSize())
                      .checksumMd5(file.getChecksumMd5())
                      .checksumCrc32c(file.getChecksumCrc32c());
                }));
      }
    }
  }

  public static void updateDirectoryEntryChecksums(
      FireStoreDirectoryEntry dirEntry, List<FireStoreDirectoryEntry> enumComputed) {
    List<String> md5Collection = new ArrayList<>();
    List<String> crc32cCollection = new ArrayList<>();
    long totalSize = 0L;

    for (FireStoreDirectoryEntry dirItem : enumComputed) {
      totalSize = totalSize + dirItem.getSize();
      if (!StringUtils.isEmpty(dirItem.getChecksumCrc32c())) {
        crc32cCollection.add(dirItem.getChecksumCrc32c().toLowerCase());
      }
      if (!StringUtils.isEmpty(dirItem.getChecksumMd5())) {
        md5Collection.add(dirItem.getChecksumMd5().toLowerCase());
      }
    }

    // Compute checksums
    // The spec is not 100% clear on the algorithm. I made specific choices on
    // how to implement it:
    // - set hex strings to lowercase before processing so we get consistent sort
    //   order and digest results.
    // - do not make leading zeros converting crc32 long to hex and it is returned
    //   in lowercase. (Matches the semantics of toHexString).
    Collections.sort(md5Collection);
    String md5Concat = StringUtils.join(md5Collection, StringUtils.EMPTY);
    String md5Checksum = computeMd5(md5Concat);

    Collections.sort(crc32cCollection);
    String crc32cConcat = StringUtils.join(crc32cCollection, StringUtils.EMPTY);
    String crc32cChecksum = computeCrc32c(crc32cConcat);

    // Update the directory in place
    dirEntry.checksumCrc32c(crc32cChecksum).checksumMd5(md5Checksum).size(totalSize);
  }

  public static String getFullPath(String dirPath, String name) {
    // Originally, this was a method in FireStoreDirectoryEntry, but the Firestore client complained
    // about it,
    // because it was not a set/get for an actual class member. Very picky, that!
    // There are three cases here:
    // - the path and name are empty: that is the root. Full path is "/"
    // - the path is "/" and the name is not empty: dir in the root. Full path is "/name"
    // - the path is "/name" and the name is not empty: Full path is path + "/" + name
    String path = StringUtils.EMPTY;
    if (StringUtils.isNotEmpty(dirPath) && !StringUtils.equals(dirPath, "/")) {
      path = dirPath;
    }
    return path + '/' + name;
  }

  public static String computeMd5(String input) {
    return StringUtils.lowerCase(DigestUtils.md5Hex(input));
  }

  public static String computeCrc32c(String input) {
    byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
    PureJavaCrc32C crc = new PureJavaCrc32C();
    crc.update(inputBytes, 0, inputBytes.length);
    return Long.toHexString(crc.getValue());
  }
}
