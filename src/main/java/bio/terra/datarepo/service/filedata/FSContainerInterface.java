package bio.terra.datarepo.service.filedata;

import bio.terra.datarepo.service.filedata.google.firestore.FireStoreProject;
import bio.terra.datarepo.service.resourcemanagement.google.GoogleProjectResource;
import java.util.UUID;

// The container interface gives the file system code a way to treat Dataset and Snapshot objects
// with a single set of code.
public interface FSContainerInterface {
  UUID getId();

  GoogleProjectResource getProjectResource();

  FireStoreProject firestoreConnection();
}
