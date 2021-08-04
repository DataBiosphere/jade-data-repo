package bio.terra.service.filedata;

import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
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
  private final FireStoreUtils fireStoreUtils;
  public static final String ROOT_DIR_NAME = "/_dr_";

  @Autowired
  public FileMetadataUtils(FireStoreUtils fireStoreUtils) {
    this.fireStoreUtils = fireStoreUtils;
  }

  // As mentioned at the top of the module, we can't use forward slash in a FireStore document
  // name, so we do this encoding.
  private static final char DOCNAME_SEPARATOR = '\u001c';

  public String encodePathAsFirestoreDocumentName(String path) {
    return StringUtils.replaceChars(path, '/', DOCNAME_SEPARATOR);
  }

  public FireStoreDirectoryEntry makeDirectoryEntry(String lookupDirPath) {
    // We have some special cases to deal with at the top of the directory tree.
    String fullPath = makePathFromLookupPath(lookupDirPath);
    String dirPath = fireStoreUtils.getDirectoryPath(fullPath);
    String objName = fireStoreUtils.getName(fullPath);
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
