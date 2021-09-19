package utils;

import java.io.Serializable;
import java.util.Objects;

public class ChunkKey implements Serializable {
    private final String fileKey;
    private final int number;

    public ChunkKey(String fileKey, int number) {
        this.fileKey = fileKey;
        this.number = number;
    }

    public int getNumber() {
        return number;
    }

    public String getFileKey() {
        return fileKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkKey chunkKey = (ChunkKey) o;
        return getNumber() == chunkKey.getNumber() && Objects.equals(getFileKey(), chunkKey.getFileKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFileKey(), getNumber());
    }
}
