package com.example;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.File;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

public class LocalApp {

    private static final String LOCAL_TO_MANAGER_QUEUE_NAME = "LocalToManagerQueue";
    private static final String MANAGER_TO_LOCAL_QUEUE_NAME_PREFIX = "ManagerToLocalQueue-";
    
    private static final String AMI_ID = "ami-0a102720738fc79c4"; 

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java -jar yourjar.jar <inputFileName> <outputFileName> <n> [terminate]");
            return;
        }
        String inputFileName = args[0];
        String outputFileName = args[1];
        String n = args[2];
        boolean terminate = (args.length > 3 && args[3].equalsIgnoreCase("terminate"));

        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();

        try {
            boolean managerActive = false;
            String nextToken = null;
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        boolean isManager = instance.tags().stream().anyMatch(t -> t.key().equals("Name") && t.value().equals("Manager"));
                        boolean isRunning = instance.state().name() == InstanceStateName.RUNNING || instance.state().name() == InstanceStateName.PENDING;
                        if (isManager && isRunning) {
                            managerActive = true;
                            break;
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null && !managerActive);

            if (!managerActive) {
                System.out.println("Manager not found. Launching a new one...");
                String script = "#!/bin/bash\n" +
                                "java -cp /home/ubuntu/demo-1.4.jar com.example.Manager\n"; 
                String userData = Base64.getEncoder().encodeToString(script.getBytes());
                RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_LARGE)
                    .imageId(AMI_ID)
                    .maxCount(1).minCount(1)
                    .keyName("vockey")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::102356500046:instance-profile/LabInstanceProfile").build())
                    .userData(userData)
                    .tagSpecifications(TagSpecification.builder().resourceType(ResourceType.INSTANCE).tags(Tag.builder().key("Name").value("Manager").build()).build())
                    .metadataOptions(InstanceMetadataOptionsRequest.builder().httpTokens(HttpTokensState.OPTIONAL).build())
                    .build();
                ec2.runInstances(runRequest);
                System.out.println("Manager launched.");
            }

            String uuid = UUID.randomUUID().toString();
            String bucketName = "dsp-bucket-" + uuid;
            String responseQueueName = MANAGER_TO_LOCAL_QUEUE_NAME_PREFIX + uuid;

            S3Helper.createBucketIfNotExists(bucketName);
            String localToManagerQueueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(LOCAL_TO_MANAGER_QUEUE_NAME).build()).queueUrl();
            String managerToLocalQueueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(responseQueueName).build()).queueUrl();

            String s3InputKey = new File(inputFileName).getName();
            S3Helper.uploadFile(bucketName, s3InputKey, inputFileName);
            
            String taskMessage = bucketName + "\t" + s3InputKey + "\t" + managerToLocalQueueUrl + "\t" + n;
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(localToManagerQueueUrl).messageBody(taskMessage).build());
            System.out.println("Task message sent.");

            System.out.println("Waiting for summary...");
            String summaryS3Key = "";
            while (true) {
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(managerToLocalQueueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20).build()).messages();
                if (!messages.isEmpty()) {
                    summaryS3Key = messages.get(0).body();
                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(managerToLocalQueueUrl).receiptHandle(messages.get(0).receiptHandle()).build());
                    break;
                }
            }

            File outputFile = new File(outputFileName);
            S3Helper.downloadFile(bucketName, summaryS3Key, outputFile.getAbsolutePath());
            System.out.println("Success! Saved to " + outputFile.getAbsolutePath());

            if (terminate) {
                System.out.println("Sending TERMINATE signal to Manager...");
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(localToManagerQueueUrl).messageBody("terminate").build());
            }

            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(managerToLocalQueueUrl).build());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
