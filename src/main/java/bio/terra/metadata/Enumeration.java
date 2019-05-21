package bio.terra.metadata;

import java.util.List;

public class Enumeration<T> {
    private int total;
    private List<T> items;

    public int getTotal() {
        return total;
    }

    public Enumeration<T> total(int total) {
        this.total = total;
        return this;
    }

    public List<T> getItems() {
        return items;
    }

    public Enumeration<T> items(List<T> items) {
        this.items = items;
        return this;
    }

}
