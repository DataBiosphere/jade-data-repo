package bio.terra.tools;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that will bulk update %23 characters in the gspath value in Firestore to # so snapshots can work.
 * Just leaving this as reference code.
 */
public class FirestoreUpdateUtil {

    private FirestoreUpdateUtil() {
    }

    public static void main(final String[] args) throws IOException, ExecutionException, InterruptedException {
        final String projectId = args[0];
        final String collectionName = args[1];

        final FirestoreOptions firestoreOptions =
            FirestoreOptions.getDefaultInstance().toBuilder()
                .setProjectId(projectId)
                .setCredentials(GoogleCredentials.getApplicationDefault())
                .build();
        final Firestore db = firestoreOptions.getService();
        final AtomicInteger i = new AtomicInteger(0);
        final AtomicInteger replaced = new AtomicInteger(0);
        final CollectionReference collection = db.collection(collectionName);
        collection
            .orderBy("fileId")
            .get()
            .get()
            .getDocuments()
            .forEach(d -> {
                if (i.incrementAndGet() % 500 == 0) {
                    System.out.printf("Processed %s documents %n", i.get());
                }
                final Map<String, Object> data = d.getData();
                final String path = String.valueOf(data.getOrDefault("gspath", ""));
                if (path.contains("%23")) {
                    final String newPath = path.replaceAll("%23", "#");
                    data.put("gspath", newPath);
                    System.out.printf("%s -> %s%n", d.getId(), newPath);
                    try {
                        collection.document(d.getId()).set(data).get();
                    } catch (Exception e) {
                        throw new RuntimeException("Error updating document", e);
                    }
                    replaced.incrementAndGet();
                }
            });

        System.out.printf("Fixed %s of %s documents%n", replaced.get(), i.get());
    }
}
