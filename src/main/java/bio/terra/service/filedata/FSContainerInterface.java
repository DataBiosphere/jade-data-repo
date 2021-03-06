package bio.terra.service.filedata;

import bio.terra.service.resourcemanagement.google.GoogleProjectResource;

import java.util.UUID;

// The container interface gives the file system code a way to treat Dataset and Snapshot objects
// with a single set of code.
public interface FSContainerInterface {
    UUID getId();
    GoogleProjectResource getProjectResource();
}
