package utils;

import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class FileUtils {
    public static final int chunkSize = 64*1000;

    public static void createDirectory(String path) {
        File directory = new File(path);
        directory.mkdirs();
    }

    public static void deleteFile(String path) {
        File file = new File(path);
        file.delete();
    }

    public static List<byte[]> fileToChunks(String path) throws IOException {
        File file = new File(path);
        FileInputStream fileStream = new FileInputStream(file);

        List<byte[]> list= new ArrayList<>();
        byte[] bytes;

        int nFullChunks = (int)file.length()/ FileUtils.chunkSize;
        int lastBytes = (int)file.length() % FileUtils.chunkSize;

        for (int i = 0; i < nFullChunks; i++) {
            bytes = new byte[FileUtils.chunkSize];
            fileStream.read(bytes, 0, FileUtils.chunkSize);
            list.add(bytes);
        }

        if (lastBytes != 0) {
            bytes = new byte[lastBytes];
            fileStream.read(bytes, 0, lastBytes);
        } else {
            bytes = new byte[0];
        }

        list.add(bytes);
        fileStream.close();

        return list;
    }

    public static byte[] fileToChunk(String path) throws IOException {
        File file = new File(path);
        FileInputStream fileStream = new FileInputStream(file);

        byte[] bytes = new byte[(int)file.length()];
        fileStream.read(bytes, 0, (int)file.length());

        fileStream.close();

        return bytes;
    }

    public static String getFileName(String path) {
        File file = new File(path);
        return file.getName();
    }

    public static void chunksToFile(List<byte[]> list, String filePath) throws IOException {
        File file = new File(filePath);
        FileUtils.createDirectory(file.getParentFile().getPath());
        file.createNewFile();

        FileOutputStream outputStream = new FileOutputStream(file);
        for (byte[] chunk : list) {
            outputStream.write(chunk);
        }
        outputStream.close();
    }

    public static void chunkToFile(byte[] chunk, String filePath) throws IOException {
        File file = new File(filePath);
        FileUtils.createDirectory(file.getParentFile().getPath());

        FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write(chunk);
        outputStream.close();
    }

    public static byte[] getFileID(String filename) throws IOException, NoSuchAlgorithmException {
        Path path = Paths.get("../" + filename);
        String hashInput = filename + Files.readAttributes(path, BasicFileAttributes.class).lastModifiedTime().toMillis();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] encoded = digest.digest(hashInput.getBytes());
        return byteToHex(encoded).getBytes(StandardCharsets.US_ASCII);
    }

    public static String byteToHex(byte[] fileId) {
        StringBuilder fileID = new StringBuilder();
        
        for (byte b : fileId) {
            char[] hexDigits = new char[2];
            hexDigits[0] = Character.forDigit((b >> 4) & 0xF, 16);
            hexDigits[1] = Character.forDigit((b & 0xF), 16);
            fileID.append(new String(hexDigits));
        }
    
        return fileID.toString();
    }
}
