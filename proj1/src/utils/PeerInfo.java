package utils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PeerInfo implements Serializable {
    public final int peerID;
    public String accessPoint;
    public final ConcurrentHashMap<String, FileInfo> filesStored = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<ChunkKey, ChunkInfo> chunksStored = new ConcurrentHashMap<>();
    private int backupSpace; // KBytes

    public PeerInfo(int peerID, String accessPoint) {
        this.peerID = peerID;
        this.accessPoint = accessPoint;
        this.backupSpace = -1;
    }

    public void setBackupSpace(int newBackupSpace) {
        backupSpace = newBackupSpace;
    }

    public int getBackupSpace() {
        return backupSpace;
    }

    public int getOccupiedSpace() {
        Set<ChunkKey> keys = chunksStored.keySet();
        int totalSpace = 0;
        for (ChunkKey key : keys)
            totalSpace += chunksStored.get(key).getSize();
        return totalSpace;
    }

    public int getFreeSpace() {
        return getBackupSpace() - getOccupiedSpace();
    }
}
