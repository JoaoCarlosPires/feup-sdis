package distributedbackup.service;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import distributedbackup.network.SSLHandler;
import distributedbackup.protocol.Message;

public abstract class Node {

    protected int id;
    protected int port;
    public SSLHandler sslHandler;
    public ScheduledExecutorService threadPool = new ScheduledThreadPoolExecutor(32);
    protected String protocolVersion;

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public void initiateSSLListener(String key){
        this.sslHandler = new SSLHandler(key, getPort(), this);
        new Thread(this.sslHandler).start();
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    protected void initSSL(String keyFile){
        System.setProperty("javax.net.ssl.keyStore", "keys/" + keyFile + ".keys");
        System.setProperty("javax.net.ssl.keyStorePassword", "123456");
        System.setProperty("javax.net.ssl.trustStore", "keys/truststore");
        System.setProperty("javax.net.ssl.trustStorePassword", "123456");

    }

    public abstract void handleMessage(Message msg, InetSocketAddress remoteAddress);
}
