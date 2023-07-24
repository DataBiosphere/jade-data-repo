package bio.terra.service.filedata;

import bio.terra.model.FileDetailModel;
import bio.terra.model.FileModel;
import bio.terra.model.FileModelType;
import bio.terra.service.filedata.FileMetadataUtils.Md5ValidationResult.Md5Type;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.exception.InvalidFileChecksumException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;

public class FileMetadataUtils {
  @VisibleForTesting protected static final String ROOT_DIR_NAME = "/_dr_";

  public FileMetadataUtils() {}

  // TODO: this currently returns the directory as "" if you pass in a one-level deep item
  // https://broadworkbench.atlassian.net/browse/DR-3005 is to fix (changing breaks tests badly)
  public static String getDirectoryPath(String path) {
    Path pathParts = Paths.get(path);
    Path parentDirectory = pathParts.getParent();
    if (pathParts.getNameCount() <= 1) {
      // We are at the root; no containing directory
      return StringUtils.EMPTY;
    }
    return parentDirectory.toString();
  }

  public static String getName(String path) {
    Path pathParts = Paths.get(path);
    Path fileName = pathParts.getFileName();
    if (fileName != null) {
      return fileName.toString();
    }
    return StringUtils.EMPTY;
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

  public static FireStoreDirectoryEntry makeDirectoryEntry(String lookupDirPath) {
    // We have some special cases to deal with at the top of the directory tree.
    String fullPath = makePathFromLookupPath(lookupDirPath);
    String dirPath = getDirectoryPath(fullPath);
    String objName = getName(fullPath);
    if (StringUtils.isEmpty(fullPath)) {
      // This is the root directory - it doesn't have a path or a name
      dirPath = StringUtils.EMPTY;
      objName = StringUtils.EMPTY;
    } else if (StringUtils.isEmpty(dirPath)) {
      // This is an entry in the root directory - it needs to have the root path.
      dirPath = "/";
    }

    return new FireStoreDirectoryEntry()
        .fileId(UUID.randomUUID().toString())
        .isFileRef(false)
        .path(dirPath)
        .name(objName)
        .fileCreatedDate(Instant.now().toString());
  }

  // Do some tidying of the full path: slash on front - no slash trailing
  // and prepend the root directory name
  public static String makeLookupPath(String fullPath) {
    String temp = StringUtils.prependIfMissing(fullPath, "/");
    temp = StringUtils.removeEnd(temp, "/");
    temp = StringUtils.prependIfMissing(temp, ROOT_DIR_NAME);
    return temp;
  }

  public static String makePathFromLookupPath(String lookupPath) {
    return StringUtils.removeStart(lookupPath, ROOT_DIR_NAME);
  }

  public static Set<String> findNewDirectoryPaths(
      List<FireStoreDirectoryEntry> datasetEntries, LRUMap<String, Boolean> pathMap) {

    Set<String> pathsToCheck = new HashSet<>();
    for (FireStoreDirectoryEntry entry : datasetEntries) {
      // Only probe the real directories - not leaf file reference or the root
      String lookupDirPath = makeLookupPath(entry.getPath());
      for (String testPath = lookupDirPath;
          !testPath.isEmpty() && !testPath.equals(FileMetadataUtils.ROOT_DIR_NAME);
          testPath = getDirectoryPath(testPath)) {

        // check the cache
        pathMap.computeIfAbsent(
            testPath,
            s -> {
              // not in the cache: add to checklist and a to cache
              pathsToCheck.add(s);
              return true;
            });
      }
    }
    return pathsToCheck;
  }

  /**
   * Given an absolute path to a file or directory, return a List of absolute paths for all parent
   * directories
   *
   * @param path An absolute path
   * @return An ordered list of paths that could potentially be created. Note: this excludes the
   *     passed in path
   */
  public static List<String> extractDirectoryPaths(String path) {
    Preconditions.checkArgument(path.startsWith("/"), "Paths should be absolute");
    List<String> allPaths = new ArrayList<>();
    String interimPath = "";
    allPaths.add("/");
    for (String part : getDirectoryPath(path).split("/")) {
      if (!StringUtils.isEmpty(part)) {
        interimPath += "/" + part;
        allPaths.add(interimPath);
      }
    }
    return allPaths;
  }

  /**
   * Results of the successful md5 comparison of user a specified MD5 against a cloud provided MD5
   *
   * @param effectiveMd5 The final md5 to record
   * @param type Which md5 was actually used
   */
  public record Md5ValidationResult(String effectiveMd5, Md5Type type) {
    public boolean isUserProvided() {
      return type().equals(Md5Type.USER_PROVIDED);
    }

    public enum Md5Type {
      USER_PROVIDED, // When the user md5 is used (e.g. when it's not present in the cloud)
      CLOUD_PROVIDED, // When the cloud md5 is used
      NEITHER // When neither value is provided
    }
  }

  /**
   * Validates that a user specified MD5 checksum matches with a cloud provided MD5 checksum. If the
   * user provided checksum is null or empty, then assume the cloud checksum is valid and return
   * that
   *
   * @param userSpecifiedMd5 A hex representation of the user specified MD5 file checksum
   * @param cloudMd5 A hex representation of the MD5 file checksum for the file object
   * @param sourcePath The cloud path where the file lives
   * @return A hex representation of the file's md5 hash
   * @throws InvalidFileChecksumException if the specified checksums don't match
   */
  public static Md5ValidationResult validateFileMd5ForIngest(
      String userSpecifiedMd5, String cloudMd5, String sourcePath)
      throws InvalidFileChecksumException {
    if (!StringUtils.isEmpty(userSpecifiedMd5)) {
      if (!StringUtils.isEmpty(cloudMd5) && !Objects.equals(cloudMd5, userSpecifiedMd5)) {
        throw new InvalidFileChecksumException(
            "Checksums do not match for file %s".formatted(sourcePath));
      }
      return new Md5ValidationResult(
          userSpecifiedMd5,
          Objects.equals(cloudMd5, userSpecifiedMd5)
              ? Md5Type.CLOUD_PROVIDED
              : Md5Type.USER_PROVIDED);
    } else {
      return new Md5ValidationResult(
          cloudMd5, StringUtils.isEmpty(cloudMd5) ? Md5Type.NEITHER : Md5Type.CLOUD_PROVIDED);
    }
  }

  public static List<FileModel> toFileModel(
      List<FireStoreDirectoryEntry> directoryEntries,
      List<FireStoreFile> files,
      String collectionId) {
    List<FileModel> resultList = new ArrayList<>();
    if (directoryEntries.size() != files.size()) {
      throw new FileSystemExecutionException("List sizes should be identical");
    }

    for (int i = 0; i < files.size(); i++) {
      FireStoreFile file = files.get(i);
      FireStoreDirectoryEntry entry = directoryEntries.get(i);

      FileModel fileModel =
          new FileModel()
              .fileId(entry.getFileId())
              .collectionId(collectionId)
              .path(FileMetadataUtils.getFullPath(entry.getPath(), entry.getName()))
              .size(file.getSize())
              .created(file.getFileCreatedDate())
              .description(file.getDescription())
              .fileType(FileModelType.FILE)
              .checksums(FileService.makeChecksums(file.getChecksumCrc32c(), file.getChecksumMd5()))
              .fileDetail(
                  new FileDetailModel()
                      .datasetId(entry.getDatasetId())
                      .accessUrl(file.getGspath())
                      .mimeType(file.getMimeType())
                      .loadTag(file.getLoadTag()));

      resultList.add(fileModel);
    }

    return resultList;
  }
}
