package endpoints.Fetch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class TopicMetadataReader {

    public boolean topicMetadataFileExists(String topicName) throws IOException {
        if (topicName != null || !topicName.isEmpty()) {
            System.out.println("Parsing cluster metadata file");
            String fileName = "/tmp/kraft-combined-logs/" + topicName + "-0/00000000000000000000.log";
            File file = new File(fileName);
            System.out.println("File exists? " + file.exists());
            return file.exists();
        }
        else {
            System.out.println("name is empty or null");
            return false;
        }
    }

    public byte[] readTopicRecords(String topicName) throws IOException {
        // TODO Actually parse the file and count records
        System.out.println("Parsing cluster metadata file");
        String fileName = "/tmp/kraft-combined-logs/" + topicName + "-0/00000000000000000000.log";
        File file = new File(fileName);
        System.out.println("File exists? " + file.exists());
        System.out.println("File total length: " + file.length());
        ByteArrayInputStream inputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
        return inputStream.readAllBytes();
    }
}
