package distributedbackup.protocol;

import distributedbackup.service.Tracker;

import java.net.InetSocketAddress;

public class TrackerCheck implements Runnable {

    private Tracker tracker;
    private String[] header;
    private long timeout = 2000;

    public TrackerCheck(Tracker tracker) {
        this.tracker = tracker;
        this.header = new String[]{tracker.getProtocolVersion(), "ALIVE", String.valueOf(tracker.getId())};
    }

    @Override
    public void run() {
        for (InetSocketAddress tracker : tracker.sslHandler.getTrackers().values()) {
            Message msg = new Message(header, this.tracker.getPort());
            this.tracker.sslHandler.send(msg, tracker);
        }

        long currentTime = System.currentTimeMillis();
        for (Integer id : tracker.sslHandler.getTimestamps().keySet()) {
            if (!this.tracker.sslHandler.getTrackers().containsKey(id))
                continue;
            long timestamp = tracker.sslHandler.getTimestamps().get(id);
            if (currentTime - timestamp > timeout) {
                tracker.sslHandler.removeTracker(id);
            }
        }
    }
}
