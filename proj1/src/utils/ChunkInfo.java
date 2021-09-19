package utils;

import java.io.Serializable;
import java.util.ArrayList;

public class ChunkInfo implements Serializable {
    private final ArrayList<Integer> peers = new ArrayList<>();
    private final int initiatorID;
    private final int desiredRepDegree;
    private boolean requested;
    private final int size;
    private boolean reclaim;

    public ChunkInfo(int initiatorID, int desiredRepDegree, int size) {
        this.initiatorID = initiatorID;
        this.desiredRepDegree = desiredRepDegree;
        this.requested = false;
        this.size = size;
        this.reclaim = false;
    }

    public int getInitiatorID() {
        return initiatorID;
    }

    public int getSize() {
        return size;
    }

    public int getDifferenceDegree() {
        return getPerceivedRepDegree() - desiredRepDegree;
    }

    public void increaseRepDegree(int peerID) {
        if (!peers.contains(peerID))
            peers.add(peerID);
    }

    public void decreaseRepDegree(int peerID) {
        if (peers.contains(peerID))
            peers.remove(Integer.valueOf(peerID));
    }

    public boolean repDegreeNotSatisfied() {
        return getPerceivedRepDegree() < desiredRepDegree;
    }

    public boolean getRequested() {
        return requested;
    }

    public void setRequested(boolean status) {
        requested = status;
    }

    public void setReclaimed(boolean status) {
        reclaim = status;
    }

    public boolean getReclaimed() {
        return reclaim;
    }

    public int getDesiredRepDegree() {
        return desiredRepDegree;
    }

    public int getPerceivedRepDegree() {
        return peers.size();
    }
}
