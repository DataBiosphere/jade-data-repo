package bio.terra.service.filedata.google.firestore;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

public class TestFirestoreProvider {

    public static Firestore getFirestore() {
        return FirestoreOptions.getDefaultInstance()
            .toBuilder()
            .setProjectId(System.getenv("GOOGLE_CLOUD_DATA_PROJECT"))
            .build()
            .getService();
    }
}
