import java.io.IOException;
import java.net.*;

public class MulticastSender {
    public static void send(byte[] data, String[] multicast) throws IOException {
        DatagramSocket socket = new DatagramSocket();
        //System.out.println("Sent packet with length " + data.length + ".");
        DatagramPacket packet = new DatagramPacket(data, data.length, InetAddress.getByName(multicast[0]), Integer.parseInt(multicast[1]));
        socket.send(packet);
        socket.close();
    }
}
