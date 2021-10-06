package bio.terra.service.filedata;

import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SnapshotComputeHelper {

  Logger logger = LoggerFactory.getLogger(SnapshotCompute.class);

  List<FireStoreFile> batchRetrieveFileMetadata(
      Map.Entry<String, List<FireStoreDirectoryEntry>> entry) throws InterruptedException;

  List<FireStoreDirectoryEntry> enumerateDirectory(String dirPath) throws InterruptedException;

  void updateEntry(FireStoreDirectoryEntry entry, List<FireStoreDirectoryEntry> updateBatch)
      throws InterruptedException;
}
