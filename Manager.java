package com.example;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

public class Manager {

    private static final String LOCAL_TO_MANAGER_QUEUE_NAME = "LocalToManagerQueue";
    private static final String MANAGER_TO_WORKERS_QUEUE_NAME = "ManagerToWorkersQueue";
    private static final String WORKER_TO_MANAGER_QUEUE_NAME = "WorkerToManagerQueue";
    
    private static final String AMI_ID = "ami-0ce32fb26f9f17806"; 

    private static SqsClient sqs;
    private static Ec2Client ec2;
    private static String localToManagerQueueUrl;
    private static String managerToWorkersQueueUrl;
    private static String workerToManagerQueueUrl;

    private static final ConcurrentHashMap<String, TaskState> activeTasks = new ConcurrentHashMap<>();
    
    private static volatile boolean terminateRequested = false;

    public static void main(String[] args) {
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        
        localToManagerQueueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(LOCAL_TO_MANAGER_QUEUE_NAME).build()).queueUrl();
        managerToWorkersQueueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(MANAGER_TO_WORKERS_QUEUE_NAME).build()).queueUrl();
        workerToManagerQueueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(WORKER_TO_MANAGER_QUEUE_NAME).build()).queueUrl();
        
        System.out.println("Manager started.");

        Thread resultListener = new Thread(new ResultListener());
        resultListener.start();

        while (!terminateRequested) {
            try {
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(localToManagerQueueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .build()).messages();
                
                for (Message message : messages) {
                    if (message.body().equals("terminate")) {
                        System.out.println("Termination signal received.");
                        terminateRequested = true;
                        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(localToManagerQueueUrl).receiptHandle(message.receiptHandle()).build());
                        break; 
                    } 
                    
                    handleNewTask(message);

                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(localToManagerQueueUrl).receiptHandle(message.receiptHandle()).build());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        shutdownManager();
    }

    private static void handleNewTask(Message message) {
        try {
            String[] parts = message.body().split("\t");
            if (parts.length < 4) return;

            String bucketName = parts[0];
            String s3InputKey = parts[1];
            String responseQueueUrl = parts[2];
            int n = Integer.parseInt(parts[3]); 

            System.out.println("New Task Received: " + bucketName);

            String inputFileContent = S3Helper.downloadFileAsString(bucketName, s3InputKey);
            List<String> jobs = parseInputFile(inputFileContent);
            
            TaskState state = new TaskState(bucketName, responseQueueUrl, jobs);
            activeTasks.put(bucketName, state);

            int workersNeeded = (int) Math.ceil((double) jobs.size() / n);
            checkAndLaunchWorkers(workersNeeded);

            for (String job : jobs) {
                sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(managerToWorkersQueueUrl)
                    .messageBody(bucketName + "\t" + job)
                    .build());
            }

        } catch (Exception e) {
            System.err.println("Error handling new task: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static class ResultListener implements Runnable {
        @Override
        public void run() {
            while (!terminateRequested || !activeTasks.isEmpty()) {
                try {
                    List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(workerToManagerQueueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(20)
                        .build()).messages();

                    for (Message msg : messages) {
                        String[] parts = msg.body().split("\t");
                        String bucketName = parts[0];

                        TaskState state = activeTasks.get(bucketName);
                        
                        if (state != null) {
                            String jobKey = parts[1] + "\t" + parts[2]; 
                            String result = parts[3];

                            if (result.startsWith("ERROR")) {
                                state.errors.put(jobKey, result);
                            } else {
                                state.completed.put(jobKey, result);
                            }

                            if (state.counter.incrementAndGet() == state.totalJobs) {
                                completeTask(state);
                                activeTasks.remove(bucketName);
                            }
                        }

                        sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(workerToManagerQueueUrl)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                    }
                } catch (Exception e) {
                   if (!terminateRequested) e.printStackTrace();
                }
            }
        }
    }

    private static void completeTask(TaskState state) {
        System.out.println("Task Completed: " + state.bucketName);
        String summary = generateHtmlSummary(state.allJobs, state.completed, state.errors, state.bucketName);
        String key = "summary.html";
        
        S3Helper.uploadString(state.bucketName, key, summary);
        
        sqs.sendMessage(SendMessageRequest.builder()
            .queueUrl(state.responseQueueUrl)
            .messageBody(key)
            .build());
    }

    private static void shutdownManager() {
        System.out.println("Shutting down Manager...");
        
        try { Thread.sleep(5000); } catch (InterruptedException e) {}

        System.out.println("Terminating Worker instances...");
        List<String> workerIds = new ArrayList<>();
        String nextToken = null;
        do {
            DescribeInstancesRequest req = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse res = ec2.describeInstances(req);
            for (Reservation r : res.reservations()) {
                for (Instance i : r.instances()) {
                    boolean isWorker = i.tags().stream().anyMatch(t -> t.key().equals("Name") && t.value().equals("Worker"));
                    if (isWorker && (i.state().name() == InstanceStateName.RUNNING || i.state().name() == InstanceStateName.PENDING)) {
                        workerIds.add(i.instanceId());
                    }
                }
            }
            nextToken = res.nextToken();
        } while (nextToken != null);

        if (!workerIds.isEmpty()) {
            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(workerIds).build());
        }

        System.out.println("Manager: attempting to terminate my own EC2 instance...");
        String selfId = getSelfInstanceId();
        
        if (selfId != null && !selfId.isEmpty()) {
            try {
                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(selfId).build());
            } catch (Exception e) {
                System.err.println("Manager: failed to terminate self: " + e.getMessage());
            }
        }
        
        System.exit(0);
    }

    private static void checkAndLaunchWorkers(int needed) {
        if (needed > 15) needed = 15; 
        int current = countActiveWorkers();
        int toLaunch = needed - current;
        if (toLaunch > 0) {
            System.out.println("Launching " + toLaunch + " workers.");
            launchWorkers(toLaunch);
        }
    }

    private static int countActiveWorkers() {
        int count = 0;
        String nextToken = null;
        do {
            DescribeInstancesRequest req = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse res = ec2.describeInstances(req);
            for (Reservation r : res.reservations()) {
                for (Instance i : r.instances()) {
                    boolean isWorker = i.tags().stream().anyMatch(t -> t.key().equals("Name") && t.value().equals("Worker"));
                    if (isWorker && (i.state().name() == InstanceStateName.RUNNING || i.state().name() == InstanceStateName.PENDING)) count++;
                }
            }
            nextToken = res.nextToken();
        } while (nextToken != null);
        return count;
    }

    private static void launchWorkers(int count) {
        String script = "#!/bin/bash\n" +
                        "java -Xmx3g -cp /home/ubuntu/demo-1.4.jar com.example.Worker\n";
        String userData = Base64.getEncoder().encodeToString(script.getBytes());

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_LARGE)
                .imageId(AMI_ID)
                .maxCount(count).minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::102356500046:instance-profile/LabInstanceProfile").build())
                .userData(userData)
                .tagSpecifications(TagSpecification.builder().resourceType(ResourceType.INSTANCE).tags(Tag.builder().key("Name").value("Worker").build()).build())
                .build();
        ec2.runInstances(runRequest);
    }

    private static String getSelfInstanceId() {
        try {
            java.net.URL url = new java.net.URL("http://169.254.169.254/latest/meta-data/instance-id");
            try (java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(url.openStream()))) {
                return in.readLine();
            }
        } catch (Exception e) {
            return null;
        }
    }

    private static List<String> parseInputFile(String fileContent) throws IOException {
        List<String> jobs = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new StringReader(fileContent));
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.trim().isEmpty() && line.contains("\t")) jobs.add(line);
        }
        return jobs;
    }

    private static String generateHtmlSummary(List<String> allJobs, Map<String, String> completedJobs, Map<String, String> jobErrors, String bucketName) {
        StringBuilder html = new StringBuilder();
        html.append("<html><body><h1>Analysis Summary</h1><ul>\n");
        for (String job : allJobs) {
            String[] parts = job.split("\t");
            String analysisType = parts[0];
            String inputUrl = parts[1];
            String jobKey = analysisType + "\t" + inputUrl; 
            
            if (completedJobs.containsKey(jobKey)) {
                String outputS3Key = completedJobs.get(jobKey);
                String outputS3Url = "https://" + bucketName + ".s3.amazonaws.com/" + outputS3Key;
                html.append("<li><b>").append(analysisType).append("</b>: ")
                    .append("<a href=\"").append(inputUrl).append("\">Input</a> | ")
                    .append("<a href=\"").append(outputS3Url).append("\">Output</a></li>\n");
            } else if (jobErrors.containsKey(jobKey)) {
                html.append("<li><b>").append(analysisType).append("</b>: <span>ERROR: ").append(jobErrors.get(jobKey)).append("</span></li>\n");
            }
        }
        html.append("</ul></body></html>\n");
        return html.toString();
    }

    static class TaskState {
        String bucketName;
        String responseQueueUrl;
        int totalJobs;
        List<String> allJobs;
        Map<String, String> completed = new ConcurrentHashMap<>();
        Map<String, String> errors = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(0);

        public TaskState(String bucketName, String responseQueueUrl, List<String> allJobs) {
            this.bucketName = bucketName;
            this.responseQueueUrl = responseQueueUrl;
            this.allJobs = allJobs;
            this.totalJobs = allJobs.size();
        }
    }
}
