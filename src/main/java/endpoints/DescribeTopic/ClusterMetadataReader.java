package endpoints.DescribeTopic;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import endpoints.DescribeTopic.models.Batch;
import endpoints.DescribeTopic.models.MetadataBatches;
import utils.ConstructorException;

public class ClusterMetadataReader {
    
        public MetadataBatches parseClusterMetadataFile() throws IOException, ConstructorException {
            System.out.println("Parsing cluster metadata file");
            String fileName = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
            File file = new File(fileName);
            System.out.println("File exists? " + file.exists());
            System.out.println("File total length: " + file.length());
            ByteArrayInputStream inputStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
            MetadataBatches batches = new MetadataBatches();
            try {
                // Read Record Batches
                while(true) {
                    // TODO Fix this check
                    System.out.println("batch availability : " + inputStream.available());
                    if(inputStream.available()<=0) {
                        System.out.println("Closing loop");
                        inputStream.close();
                        break;
                    }
                    Batch batch = new Batch(inputStream);
                    batches.batchesArray.add(batch);
                    System.out.println("Added batch, now we have " + batches.batchesArray.size());
                }
                return batches;
            }
            finally {
                if(inputStream != null) {
                    try {
                        inputStream.close();
                    }
                    catch(IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }
