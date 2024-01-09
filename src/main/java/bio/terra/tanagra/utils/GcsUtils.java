package bio.terra.tanagra.utils;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public final class GcsUtils {

  private GcsUtils() {}

  public static String createSignedUrl(String projectId, String bucketName, String fileName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, fileName)).build();

    URL url =
        storage.signUrl(blobInfo, 30, TimeUnit.MINUTES, Storage.SignUrlOption.withV4Signature());
    return url.toString();
  }
}
