package distributedbackup.protocol;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class Message implements Serializable  {

    private final String protocolVersion;
    private final String messageType;
    private final int senderID;
    private final String fileID;
    private final int chunkNo;
    private final int replicationDegree;
    private final int port;
    private final String peerIP;
    private final int peerPort;
    private  byte[] body;

    public Message (String[] header, byte[] body, int port){
        this.protocolVersion = header[0];
        this.messageType = header[1];
        this.senderID = Integer.parseInt(header[2]);
        this.port = port;
        this.fileID = (header.length >= 5) ? header[3] : "";
        this.chunkNo = (header.length >= 5) ? Integer.parseInt(header[4]) : -1;
        this.replicationDegree = (header.length >= 6) ? Integer.parseInt(header[5]) : -1;
        this.body = (body != null) ? body : new byte[0];
        this.peerIP = null;
        this.peerPort = -1;
    }

    public Message (String[] header, byte[] body, InetSocketAddress add){
        this.protocolVersion = header[0];
        this.messageType = header[1];
        this.senderID = Integer.parseInt(header[2]);
        this.port = -1;
        this.fileID = (header.length >= 5) ? header[3] : "";
        this.chunkNo = (header.length >= 5) ? Integer.parseInt(header[4]) : -1;
        this.replicationDegree = (header.length >= 6) ? Integer.parseInt(header[5]) : -1;
        this.body = (body != null) ? body : new byte[0];
        String ip = add.getAddress().toString();
        this.peerIP = ip.substring(ip.lastIndexOf('/') + 1);
        this.peerPort = add.getPort();
    }

    public Message (String[] header, int port){
        this.protocolVersion = header[0];
        this.messageType = header[1];
        this.senderID = Integer.parseInt(header[2]);
        this.port = port;
        this.fileID = "";
        this.chunkNo = -1;
        this.replicationDegree = -1;
        this.body = new byte[0];
        this.peerIP = null;
        this.peerPort =-1;
    }

    public byte[] getBody() {
        return body;
    }

    public int getChunkNo() {
        return chunkNo;
    }

    public int getReplicationDegree() {
        return replicationDegree;
    }

    public int getSenderID() {
        return senderID;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getFileID() {
        return fileID;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public String getPeerIP() {
        return peerIP;
    }

    public int getPeerPort() {
        return peerPort;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {

        return "Message header:" +
                " protocolVersion='" + this.getProtocolVersion() + '\'' +
                " messageType='" + this.getMessageType() + '\'' +
                " senderId=" + this.senderID +
                " fileID='" + this.fileID + '\'' +
                " chunkNo=" + this.chunkNo +
                " replicationDegree=" + this.getReplicationDegree() +
                " body length: " + this.getBody().length +
                " port " + this.port;
    }

}
