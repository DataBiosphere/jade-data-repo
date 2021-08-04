package bio.terra.service.filedata;

import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class FileMetadataUtils {
  private final Logger logger = LoggerFactory.getLogger(FileMetadataUtils.class);
  public static final String ROOT_DIR_NAME = "/_dr_";

  @Autowired
  public FileMetadataUtils() { }

  // As mentioned at the top of the module, we can't use forward slash in a FireStore document
  // name, so we do this encoding.
  private static final char DOCNAME_SEPARATOR = '\u001c';

  public String encodePathAsFirestoreDocumentName(String path) {
    return StringUtils.replaceChars(path, '/', DOCNAME_SEPARATOR);
  }

  public String getDirectoryPath(String path) {
    String[] pathParts = StringUtils.split(path, '/');
    if (pathParts.length <= 1) {
      // We are at the root; no containing directory
      return StringUtils.EMPTY;
    }
    int endIndex = pathParts.length - 1;
    return '/' + StringUtils.join(pathParts, '/', 0, endIndex);
  }

  public String getName(String path) {
    String[] pathParts = StringUtils.split(path, '/');
    if (pathParts.length == 0) {
      return StringUtils.EMPTY;
    }
    return pathParts[pathParts.length - 1];
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
