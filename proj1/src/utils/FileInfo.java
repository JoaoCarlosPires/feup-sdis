package utils;

import java.io.Serializable;
import java.util.ArrayList;

public class FileInfo implements Serializable {
    public final ArrayList<ArrayList<Integer>> chunks = new ArrayList<>();
    private final int desiredRepDegree;
    private final String fileName;

    public FileInfo(String fileName, int nChunks, int desiredRepDegree) {
        this.fileName = fileName;
        this.desiredRepDegree = desiredRepDegree;
        for (int i = 0; i < nChunks; i++) {
            chunks.add(new ArrayList<>());
        }
    }

    public boolean repDegreeNotSatisfied(int chunkNo) {
        return chunks.get(chunkNo).size() < desiredRepDegree;
    }

    public boolean notStored() {
        for (ArrayList<Integer> peers : chunks)
            if (peers.size() == 0)
                return true;
        return false;
    }

    public String getFileName() {
        return fileName;
    }

    public int getDesiredRepDegree() {
        return desiredRepDegree;
    }
}
