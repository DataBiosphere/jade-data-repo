package bio.terra.service.filedata;

import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FileMetadataUtils {
  public static final String ROOT_DIR_NAME = "/_dr_";

  @Autowired
  public FileMetadataUtils() {}

  public String getDirectoryPath(String path) {
    Path pathParts = Paths.get(path);
    Path parentDirectory = pathParts.getParent();
    if (parentDirectory != null) {
      return parentDirectory.toString();
    }
    // We are at the root; no containing directory
    return StringUtils.EMPTY;
  }

  public String getName(String path) {
    Path pathParts = Paths.get(path);
    Path fileName = pathParts.getFileName();
    if (fileName != null) {
      return fileName.toString();
    }
    return StringUtils.EMPTY;
  }

  public String getFullPath(String dirPath, String name) {
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

  public FireStoreDirectoryEntry makeDirectoryEntry(String lookupDirPath) {
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
  public String makeLookupPath(String fullPath) {
    String temp = StringUtils.prependIfMissing(fullPath, "/");
    temp = StringUtils.removeEnd(temp, "/");
    return ROOT_DIR_NAME + temp;
  }

  public String makePathFromLookupPath(String lookupPath) {
    return StringUtils.removeStart(lookupPath, ROOT_DIR_NAME);
  }
}
