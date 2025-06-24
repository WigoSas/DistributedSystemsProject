package it.unitn.ds1;

import java.io.Serializable;

public class Element implements Serializable {
    int key;
    String data;
    int version;

    public Element (int key, String data, int version) {
        this.key = key;
        this.data = data;
        this.version = version;
    }

}
