package bio.terra.dao;

import java.util.List;

public interface MetaDao<T> {
    void create(T object);
    T retrieve(String id);
    //void update(T object);
    void delete(String id);
    List<T> enumerate();
}
