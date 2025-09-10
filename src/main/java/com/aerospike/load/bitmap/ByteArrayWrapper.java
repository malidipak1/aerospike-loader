package com.aerospike.load.bitmap;

import java.util.Arrays;

public class ByteArrayWrapper {
    private final byte[] data;
    private final int hash;

    ByteArrayWrapper(byte[] data) {
        this.data = data;
        this.hash = Arrays.hashCode(data);
    }

    byte[] get() { return data; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteArrayWrapper)) return false;
        return Arrays.equals(data, ((ByteArrayWrapper)o).data);
    }
    @Override public int hashCode() { return hash; }
}
