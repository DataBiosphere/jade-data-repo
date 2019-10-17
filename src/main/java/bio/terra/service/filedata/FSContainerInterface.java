package bio.terra.service.filedata;

import java.util.UUID;

// The container interface gives the file system code a way to treat Dataset and Snapshot objects
// with a single set of code.
public interface FSContainerInterface {
    String getDataProjectId();
    UUID getId();
}
