package br.jpiccoli;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class DownloadAndStoreToS3CLI {

    public static void main(String[] args) throws IOException, InterruptedException {

        AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(args[0], args[1]));

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        s3ClientBuilder.setCredentials(credentialsProvider);
        s3ClientBuilder.setRegion(Regions.US_EAST_2.getName());

        DownloadAndStoreToS3 downloadAndStoreToS3 = new DownloadAndStoreToS3(s3ClientBuilder.build());
        downloadAndStoreToS3.setProgressListener(new DefaultProgressListener());
        downloadAndStoreToS3.setUploadRetryHandler(new CustomRetryHandler());
        downloadAndStoreToS3.setDownloadRetryHandler(new CustomRetryHandler());

        String result = downloadAndStoreToS3.execute(args[2], args[3], args[4]);
        System.out.println(">>> Succeeded! Stored object ETag: " + result);

    }

    public static class DefaultProgressListener implements ProgressListener {

        @Override
        public void transferProgress(long totalTransferred, long contentLength) {

            String measurementUnit = "bytes";
            double totalTransferredDouble = (double) totalTransferred;
            if (totalTransferredDouble > 1024.0d) {
                totalTransferredDouble = totalTransferredDouble / 1024.0d;
                measurementUnit = "KB";
                if (totalTransferredDouble > 1024.0d) {
                    totalTransferredDouble = totalTransferredDouble / 1024.0d;
                    measurementUnit = "MB";
                    if (totalTransferredDouble > 1024.0d) {
                        totalTransferredDouble = totalTransferredDouble / 1024.0d;
                        measurementUnit = "GB";
                    }
                }
            }

            BigDecimal totalTransferredBD = new BigDecimal(totalTransferredDouble);
            totalTransferredBD = totalTransferredBD.setScale(2, RoundingMode.HALF_DOWN);

            System.out.print("> Transferred data count: " + totalTransferredBD.toString() + " " + measurementUnit);

            if (contentLength > 0) {
                double percentTransferred = ((double) totalTransferred) / ((double) contentLength) * 100.0d;
                BigDecimal percentTransferredBD = new BigDecimal(percentTransferred);
                percentTransferredBD = percentTransferredBD.setScale(2, RoundingMode.HALF_DOWN);
                System.out.println(" - Overall progress: " + percentTransferredBD.toString() + "%");
            }

        }

        @Override
        public void transferRateUpdate(int bytesTransferred, long timeDeltaInMs) {

            double timeDeltaInSeconds = timeDeltaInMs / 1000.0d;
            double dataTransferRate = bytesTransferred / timeDeltaInSeconds;
            String measurementUnit = "B/s";

            if (dataTransferRate > 1024.0d) {
                dataTransferRate = dataTransferRate / 1024.0d;   // KB/s
                measurementUnit = "KB/s";
                if (dataTransferRate > 1024.0d) {
                    dataTransferRate = dataTransferRate / 1024.0d;   // MB/s
                    measurementUnit = "MB/s";
                    if (dataTransferRate > 1024.0d) {
                        dataTransferRate = dataTransferRate / 1024.0d;   // GB/s
                        measurementUnit = "GB/s";
                    }
                }
            }

            BigDecimal dataTransferRateBD = new BigDecimal(String.valueOf(dataTransferRate));
            dataTransferRateBD = dataTransferRateBD.setScale(2, RoundingMode.HALF_DOWN);

            System.out.println("> Transfer rate: " + dataTransferRateBD.toString() + " " + measurementUnit);

        }

    }

    public static class CustomRetryHandler implements RetryHandler {

        @Override
        public boolean shouldRetry(Exception e) {
            e.printStackTrace();
            return true;
        }

    }

}
