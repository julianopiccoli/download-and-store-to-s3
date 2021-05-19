package br.jpiccoli;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.util.Md5Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class DownloadAndStoreToS3 {

    private final AmazonS3 s3Client;

    private int chunkSize = 1024 * 1024 * 128;
    private int maxTryCount = 30;
    private long intervalBetweenTries = 10000;
    private StorageClass storageClass = StorageClass.DeepArchive;

    private ProgressListener progressListener;
    private RetryHandler uploadRetryHandler = new DefaultRetryHandler();
    private RetryHandler downloadRetryHandler = new DefaultRetryHandler();

    public DownloadAndStoreToS3(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getMaxTryCount() {
        return maxTryCount;
    }

    public void setMaxTryCount(int maxTryCount) {
        this.maxTryCount = maxTryCount;
    }

    public long getIntervalBetweenTries() {
        return intervalBetweenTries;
    }

    public void setIntervalBetweenTries(long intervalBetweenTries) {
        this.intervalBetweenTries = intervalBetweenTries;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(StorageClass storageClass) {
        this.storageClass = storageClass;
    }

    public ProgressListener getProgressListener() {
        return progressListener;
    }

    public void setProgressListener(ProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    public RetryHandler getUploadRetryHandler() {
        return uploadRetryHandler;
    }

    public void setUploadRetryHandler(RetryHandler uploadRetryHandler) {
        if (uploadRetryHandler == null) {
            uploadRetryHandler = new DefaultRetryHandler();
        }
        this.uploadRetryHandler = uploadRetryHandler;
    }

    public RetryHandler getDownloadRetryHandler() {
        return downloadRetryHandler;
    }

    public void setDownloadRetryHandler(RetryHandler downloadRetryHandler) {
        if (downloadRetryHandler == null) {
            downloadRetryHandler = new DefaultRetryHandler();
        }
        this.downloadRetryHandler = downloadRetryHandler;
    }

    public String execute(String urlText, String bucket, String fileName) throws IOException, InterruptedException {

        boolean objectExists = s3Client.doesObjectExist(bucket, fileName);

        if (!objectExists) {

            HttpURLConnection connection = openConnection(urlText, 0);
            long fullContentLength = connection.getContentLengthLong();
            String contentType = connection.getContentType();

            ObjectMetadata metadata = new ObjectMetadata();
            if (fullContentLength > 0) {
                metadata.setContentLength(fullContentLength);
            }
            if (contentType != null) {
                metadata.setContentType(connection.getContentType());
            }

            InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucket, fileName, metadata);
            initiateMultipartUploadRequest.withStorageClass(storageClass);
            InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client.initiateMultipartUpload(initiateMultipartUploadRequest);

            // The chunkSize value must be kept stable during the method execution, so we store it in a local variable
            // and use its value instead of the instance field.
            int localChunkSize = chunkSize;
            // The extra 10 bytes at the end of the buffer allows the code to do a multipart upload without knowing
            // the full length of the stream in advance.
            byte[] buffer = new byte[localChunkSize + 10];

            InputStream stream = connection.getInputStream();

            int readResult = stream.read(buffer);
            int partNumber = 1;
            int totalReadInCurrentChunk = 0;
            long totalStreamReadCount = 0;
            long chunkStartTimestamp = System.currentTimeMillis();
            List<UploadPartResult> uploadPartResultList = new ArrayList<>();

            while (readResult >= 0) {
                totalReadInCurrentChunk += readResult;
                if (totalReadInCurrentChunk > localChunkSize) {
                    long chunkEndTimestamp = System.currentTimeMillis();
                    totalStreamReadCount += localChunkSize;
                    UploadPartResult uploadPartResult = uploadPart(initiateMultipartUploadResult.getUploadId(),
                        initiateMultipartUploadRequest.getBucketName(),
                        initiateMultipartUploadRequest.getKey(), buffer, localChunkSize, partNumber++, false);
                    uploadPartResultList.add(uploadPartResult);
                    fireTransferRateUpdate(localChunkSize, chunkEndTimestamp - chunkStartTimestamp);
                    fireTransferProgress(totalStreamReadCount, metadata.getContentLength());
                    System.arraycopy(buffer, localChunkSize, buffer, 0, totalReadInCurrentChunk - localChunkSize);
                    totalReadInCurrentChunk -= localChunkSize;
                    chunkStartTimestamp = chunkEndTimestamp;
                }
                try {
                    readResult = stream.read(buffer, totalReadInCurrentChunk, buffer.length - totalReadInCurrentChunk);
                } catch (IOException e) {
                    connection = openConnection(urlText, totalStreamReadCount);
                    stream = connection.getInputStream();
                    readResult = 0;
                    totalReadInCurrentChunk = 0;
                }
            }

            if (totalReadInCurrentChunk > 0) {
                totalStreamReadCount += totalReadInCurrentChunk;
                UploadPartResult uploadPartResult = uploadPart(initiateMultipartUploadResult.getUploadId(),
                    initiateMultipartUploadRequest.getBucketName(),
                    initiateMultipartUploadRequest.getKey(), buffer, totalReadInCurrentChunk, partNumber, true);
                uploadPartResultList.add(uploadPartResult);
                fireTransferProgress(totalStreamReadCount, metadata.getContentLength());
            }

            CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest();
            completeMultipartUploadRequest.withUploadId(initiateMultipartUploadResult.getUploadId())
                .withBucketName(initiateMultipartUploadRequest.getBucketName())
                .withKey(initiateMultipartUploadRequest.getKey())
                .withPartETags(uploadPartResultList);
            CompleteMultipartUploadResult completeMultipartUploadResult = s3Client.completeMultipartUpload(completeMultipartUploadRequest);
            return completeMultipartUploadResult.getETag();

        } else {
            throw new IOException("There is another object with the same key in the same bucket");
        }

    }

    private HttpURLConnection openConnection(String urlText, long offset) throws InterruptedException, IOException {

        for (int tryCount = 0; tryCount < maxTryCount; tryCount++) {

            try {

                URL url = new URL(urlText);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setAllowUserInteraction(false);
                connection.setConnectTimeout(10000);
                connection.setReadTimeout(10000);
                connection.setInstanceFollowRedirects(true);
                connection.setDoInput(true);

                if (offset > 0) {
                    connection.addRequestProperty("Range", "bytes " + offset + "-");
                }

                int responseCode = connection.getResponseCode();
                if (responseCode >= 200 && responseCode < 300) {
                    return connection;
                } else {
                    ByteArrayOutputStream responseStorage = new ByteArrayOutputStream();
                    InputStream stream = connection.getErrorStream();
                    if (stream == null) {
                        stream = connection.getInputStream();
                    }

                    if (stream != null) {

                        byte[] buffer = new byte[1024 * 10];
                        int readResult = stream.read(buffer);
                        while (readResult > 0) {
                            responseStorage.write(buffer);
                            readResult = stream.read(buffer);
                        }
                        throw new IOException("Invalid response code: " + responseCode + " Body: " + new String(buffer));
                    } else {
                        throw new IOException("Invalid response code without body: " + responseCode);
                    }
                }

            } catch (IOException e) {
                if (!downloadRetryHandler.shouldRetry(e)) {
                    throw e;
                }
            }

            Thread.sleep(intervalBetweenTries);

        }

        throw new IOException("Giving up after " + maxTryCount + " connection attempts");

    }

    private UploadPartResult uploadPart(String uploadId, String bucketName, String key, byte[] buffer, int length, int partNumber,
                                        boolean lastPart) throws IOException, InterruptedException {
        for (int tryCount = 0; tryCount < maxTryCount; tryCount++) {
            try {
                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.withUploadId(uploadId)
                    .withBucketName(bucketName)
                    .withKey(key)
                    .withInputStream(new ByteArrayInputStream(buffer, 0, length))
                    .withPartNumber(partNumber)
                    .withPartSize(length)
                    .withMD5Digest(Md5Utils.md5AsBase64(new ByteArrayInputStream(buffer, 0, length)))
                    .withLastPart(lastPart);
                return s3Client.uploadPart(uploadPartRequest);
            } catch (SdkClientException e) {
                if (!uploadRetryHandler.shouldRetry(e)) {
                    throw e;
                }
            }
            Thread.sleep(intervalBetweenTries);
        }
        throw new IOException("Giving up after " + maxTryCount + " upload attempts");
    }

    private void fireTransferProgress(long totalTransferred, long contentLength) {
        ProgressListener localProgressListener = progressListener;
        if (localProgressListener != null) {
            localProgressListener.transferProgress(totalTransferred, contentLength);
        }
    }

    private void fireTransferRateUpdate(int bytesTransferred, long timeDeltaInMs) {
        ProgressListener localProgressListener = progressListener;
        if (localProgressListener != null) {
            localProgressListener.transferRateUpdate(bytesTransferred, timeDeltaInMs);
        }
    }

}
