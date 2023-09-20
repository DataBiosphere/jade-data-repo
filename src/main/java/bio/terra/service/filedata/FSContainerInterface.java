package bio.terra.service.filedata;

import bio.terra.common.CollectionType;
import bio.terra.model.CloudPlatform;
import bio.terra.service.filedata.google.firestore.FireStoreProject;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import java.util.UUID;

// The container interface gives the file system code a way to treat Dataset and Snapshot objects
// with a single set of code.
public interface FSContainerInterface {
  UUID getId();

  GoogleProjectResource getProjectResource();

  FireStoreProject firestoreConnection();

  String getName();

  CollectionType getCollectionType();

  CloudPlatform getCloudPlatform();

  boolean isDataset();

  boolean isSnapshot();
}
