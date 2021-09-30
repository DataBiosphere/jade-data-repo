package bio.terra.service.filedata;

import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;

public class FileMetadataUtils {
  public static final String ROOT_DIR_NAME = "/_dr_";

  public FileMetadataUtils() {}

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

  public static List<String> findNewDirectoryPaths(
      List<FireStoreDirectoryEntry> datasetEntries, LRUMap<String, Boolean> pathMap) {

    List<String> pathsToCheck = new ArrayList<>();
    for (FireStoreDirectoryEntry entry : datasetEntries) {
      // Only probe the real directories - not leaf file reference or the root
      String lookupDirPath = makeLookupPath(entry.getPath());
      for (String testPath = lookupDirPath;
          !testPath.isEmpty() && !testPath.equals(FileMetadataUtils.ROOT_DIR_NAME);
          testPath = getDirectoryPath(testPath)) {

        // check the cache
        if (pathMap.get(testPath) == null) {
          // not in the cache: add to checklist and a to cache
          pathsToCheck.add(testPath);
          pathMap.put(testPath, true);
        }
      }
    }
    return pathsToCheck;
  }
}
