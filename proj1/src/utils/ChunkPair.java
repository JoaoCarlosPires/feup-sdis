package utils;

public class ChunkPair implements Comparable<ChunkPair> {
    private final ChunkKey chunkKey;
    private final int repDifference;

    public ChunkPair(ChunkKey chunkKey, int repDifference) {
        this.chunkKey = chunkKey;
        this.repDifference = repDifference;
    }

    public ChunkKey getKey() {
        return chunkKey;
    }

    public int getValue() {
        return repDifference;
    }

    @Override
    public int compareTo(ChunkPair o) {
        return Integer.compare(this.repDifference, o.getValue());
    }
}
