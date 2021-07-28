package bio.terra.service.filedata.google.gcs;

import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class GcsIO {

  public static String getBlobContents(Storage storage, String projectId, BlobInfo blobInfo) {
    var blob = storage.get(blobInfo.getBlobId(), Storage.BlobGetOption.userProject(projectId));
    var contents = blob.getContent(Blob.BlobSourceOption.userProject(projectId));
    return new String(contents, StandardCharsets.UTF_8);
  }

  public static int writeBlobContents(
      Storage storage, String projectId, BlobInfo blobInfo, String contents) {
    var blob = storage.get(blobInfo.getBlobId(), Storage.BlobGetOption.userProject(projectId));
    try (var writer = blob.writer(Storage.BlobWriteOption.userProject(projectId))) {
      return writer.write(ByteBuffer.wrap(contents.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException ex) {
      throw new GoogleResourceException(
          String.format("Could not write to GCS file at %s", GcsPdao.getGsPathFromBlob(blobInfo)),
          ex);
    }
  }

  public static int writeBlobContents(
      Storage storage, String projectId, String gsPath, String contents) {
    return writeBlobContents(
        storage, projectId, GcsPdao.getBlobFromGsPath(storage, gsPath, projectId), contents);
  }
}
