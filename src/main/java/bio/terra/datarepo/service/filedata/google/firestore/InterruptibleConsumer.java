package bio.terra.datarepo.service.filedata.google.firestore;

@FunctionalInterface
public interface InterruptibleConsumer<T> {
  void accept(T t) throws InterruptedException;
}
