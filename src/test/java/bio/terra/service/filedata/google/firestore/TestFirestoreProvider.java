package bio.terra.service.filedata.google.firestore;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

public final class TestFirestoreProvider {

  private TestFirestoreProvider() {}

  public static Firestore getFirestore(String googleProjectId) {
    return FirestoreOptions.getDefaultInstance().toBuilder()
        .setProjectId(googleProjectId)
        .build()
        .getService();
  }
}
