package com.example;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3Helper {

    private static final Region REGION = Region.US_EAST_1;
    private static final S3Client s3 = S3Client.builder().region(REGION).build();

    /**
     * Uploads a local file to S3.
     * Used by LocalApp.
     */
    public static void uploadFile(String bucketName, String key, String localFilePath) {
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(),
            RequestBody.fromFile(Paths.get(localFilePath)));
    }

    /**
     * Uploads a simple String as a new text file to S3.
     * Used by Manager (for HTML) and Worker (for analysis results).
     */
    public static void uploadString(String bucketName, String key, String content) {
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(),
            RequestBody.fromString(content, StandardCharsets.UTF_8));
    }

    /**
     * Downloads an S3 object to a local file.
     * Used by LocalApp to get the final summary.
     */
    public static void downloadFile(String bucketName, String key, String localFilePath) {
        s3.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(),
            ResponseTransformer.toFile(Paths.get(localFilePath)));
    }

    /**
     * Downloads an S3 object and returns its content as a String.
     * Used by Manager to read the input file.
     */
    public static String downloadFileAsString(String bucketName, String key) {
        return s3.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(),
            ResponseTransformer.toBytes()).asUtf8String();
    }

    public static void createBucketIfNotExists(String bucketName) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            System.out.println("Bucket " + bucketName + " already exists.");
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                try {
                    s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                    System.out.println("Created bucket: " + bucketName);
                } catch (S3Exception createEx) {
                    System.err.println("Failed to create bucket: " + bucketName);
                    throw createEx;
                }
            } else {
                System.err.println("Could not check bucket existence: " + e.getMessage());
                throw e;
            }
        }
    }
}
