package bio.terra.datarepo.service.filedata.google.firestore;

import com.google.api.core.ApiFuture;

@FunctionalInterface
public interface ApiFutureGenerator<T, V> {
  ApiFuture<T> accept(V input) throws InterruptedException;
}
