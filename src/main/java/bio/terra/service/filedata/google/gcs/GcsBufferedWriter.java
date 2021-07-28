package bio.terra.service.filedata.google.gcs;

import com.google.cloud.storage.Storage;
import java.io.BufferedWriter;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

/** Given a gs path and a storage object, return a buffered writer for the blob */
public class GcsBufferedWriter extends BufferedWriter {
  public GcsBufferedWriter(Storage storage, String projectId, String gsPath) {
    super(
        Channels.newWriter(
            GcsPdao.getBlobFromGsPath(storage, gsPath, projectId).writer(),
            StandardCharsets.UTF_8.name()));
  }
}
