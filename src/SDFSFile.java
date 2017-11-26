import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by haosun on 11/25/17.
 * SDFS File
 */
public class SDFSFile implements Serializable{
    private String fileName;
    private int timeStamp;
    //SHA256 is the hash result of the file content. If the SHA256 is the same, that means
    //file content is exactly the same.
    private String SHA256;
    //file content
    private byte[] file;
    private long fileSize;
    private long lastModificationTime;

    public SDFSFile(String fileName, int timeStamp, String SHA256, long fileSize, long lastModificationTime) {
        this.fileName = fileName;
        this.timeStamp = timeStamp;
        this.SHA256 = SHA256;
        this.fileSize = fileSize;
        this.lastModificationTime = lastModificationTime;
    }

    public SDFSFile(String fileName, String localFileName, int timeStamp) throws IOException, NoSuchAlgorithmException {
        this.fileName = fileName;
        this.timeStamp = timeStamp;
        this.file = Files.readAllBytes(Paths.get(localFileName));
        this.fileSize = this.file.length;
        this.lastModificationTime = System.currentTimeMillis();

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(this.file);
        byte[] digest = md.digest();
        this.SHA256 = String.format("%064x", new java.math.BigInteger(1, digest));
    }

    /**
     * implements serializable
     * @param out object output stream
     * @throws IOException io exception
     */
    private void writeObject (java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(fileName);
        out.writeInt(timeStamp);
        out.writeObject(SHA256);
        out.writeObject(file);
        out.writeLong(fileSize);
        out.writeLong(lastModificationTime);
    }

    /**
     * implements serializable
     * @param in object input stream
     * @throws IOException io exception
     * @throws ClassNotFoundException class not found exception
     */
    private void readObject (java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        fileName = (String) in.readObject();
        timeStamp = in.readInt();
        SHA256 = (String) in.readObject();
        file = (byte[]) in.readObject();
        fileSize = in.readLong();
        lastModificationTime = in.readLong();
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(int timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getSHA256() {
        return SHA256;
    }

    public void setSHA256(String SHA256) {
        this.SHA256 = SHA256;
    }

    public byte[] getFile() {
        return file;
    }

    public void setFile(byte[] file) {
        this.file = file;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getLastModificationTime() {
        return lastModificationTime;
    }

    public void setLastModificationTime(long lastModificationTime) {
        this.lastModificationTime = lastModificationTime;
    }
}
