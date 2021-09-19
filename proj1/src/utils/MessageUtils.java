package utils;

import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class MessageUtils {
    public static final int maxHeaderSize = 100;
    public static final String protocolVersion1 = "1.0";
    public static final String protocolVersion2 = "2.0";
    private static final byte[] space = " ".getBytes(StandardCharsets.ISO_8859_1);
    private static final byte[] crlf = "\r\n".getBytes(StandardCharsets.ISO_8859_1);

    public enum MessageType {
        PUTCHUNK,
        STORED,
        GETCHUNK,
        CHUNK,
        DELETE,
        REMOVED
    }

    public static byte[] addBody(byte[] header, byte[] body) throws IOException{
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(header);
        outputStream.write(body);
        return outputStream.toByteArray();
    }
    
    public static byte[] createHeader(String protocolVersion, MessageType msgType, int senderID, byte[] fileID, int chunkNo, int repDegree) throws IOException {
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(protocolVersion.getBytes(StandardCharsets.US_ASCII));
        outputStream.write(space);

        switch (msgType) {
            case PUTCHUNK: {
                outputStream.write("PUTCHUNK".getBytes(StandardCharsets.US_ASCII));
                break;
            }
            case STORED: {
                outputStream.write("STORED".getBytes(StandardCharsets.US_ASCII));
                break;
            }
            case GETCHUNK: {
                outputStream.write("GETCHUNK".getBytes(StandardCharsets.US_ASCII));
                break;
            }
            case CHUNK: {
                outputStream.write("CHUNK".getBytes(StandardCharsets.US_ASCII));
                break;
            }
            case DELETE: {
                outputStream.write("DELETE".getBytes(StandardCharsets.US_ASCII));
                break;
            }
            case REMOVED: {
                outputStream.write("REMOVED".getBytes(StandardCharsets.US_ASCII));
                break;
            }
            default:
                return new byte[0];
        }
        
        outputStream.write(space);
        outputStream.write(String.valueOf(senderID).getBytes(StandardCharsets.US_ASCII));
        outputStream.write(space);
        outputStream.write(fileID);
        outputStream.write(space);
        if (chunkNo != -1) {
            outputStream.write(String.valueOf(chunkNo).getBytes(StandardCharsets.US_ASCII));
            outputStream.write(space);
        }
        if (repDegree != -1) {
            outputStream.write(String.valueOf(repDegree).getBytes(StandardCharsets.US_ASCII));
            outputStream.write(space);
        }
        outputStream.write(crlf);
        outputStream.write(crlf);
        return outputStream.toByteArray();
    }

    public static ArrayList<byte[]> parseMessage(byte[] message) {
        String m = new String(message, StandardCharsets.ISO_8859_1);
        String[] header_body = m.split("\r\n\r\n",2);

        String[] header = header_body[0].split(" ");
        
        ArrayList<byte[]> parsedMessage = new ArrayList<>();
        for (String elem : header) {
            parsedMessage.add(elem.getBytes(StandardCharsets.ISO_8859_1));
        }

        if (header_body.length > 1) {
            byte[] body = header_body[1].getBytes(StandardCharsets.ISO_8859_1);
            parsedMessage.add(body);
        }

        return parsedMessage;
    }

    public static int byteArrayToInt(byte[] b) {
        return  Integer.parseInt(new String(b));    
    }

}
