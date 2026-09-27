package com.example;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.regions.Region;

import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class Worker {

    private static final String MANAGER_TO_WORKERS_QUEUE_NAME = "ManagerToWorkersQueue";
    private static final String WORKER_TO_MANAGER_QUEUE_NAME = "WorkerToManagerQueue";

    private static StanfordCoreNLP posPipeline;
    private static StanfordCoreNLP constituencyPipeline;
    private static StanfordCoreNLP dependencyPipeline;

    public static void main(String[] args) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        
        String managerToWorkersQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
            .queueName(MANAGER_TO_WORKERS_QUEUE_NAME).build()).queueUrl();
        String workerToManagerQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
            .queueName(WORKER_TO_MANAGER_QUEUE_NAME).build()).queueUrl();

        initializePipelines();
        System.out.println("Worker is up. Listening on: " + managerToWorkersQueueUrl);

        while (true) {
            String bucketName = null;
            String analysisType = null;
            String textUrl = null;
            Message jobMessage = null;

            try {
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(managerToWorkersQueueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20) 
                    .visibilityTimeout(300) 
                    .build()).messages();

                if (messages.isEmpty()) continue;
                
                jobMessage = messages.get(0);
                
                String[] parts = jobMessage.body().split("\t");
                if (parts.length < 3) {
                     System.err.println("Invalid message format: " + jobMessage.body());
                     sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(managerToWorkersQueueUrl)
                        .receiptHandle(jobMessage.receiptHandle())
                        .build());
                     continue;
                }

                bucketName = parts[0];
                analysisType = parts[1];
                textUrl = parts[2];

                System.out.println("Job Received: " + analysisType + " | " + textUrl + " (Client: " + bucketName + ")");

                String analysisResult = performAnalysis(analysisType, textUrl);
                
                String outputS3Key = "results/" + analysisType + "-" + System.currentTimeMillis() + ".txt";
                S3Helper.uploadString(bucketName, outputS3Key, analysisResult);

                String doneMessage = bucketName + "\t" + analysisType + "\t" + textUrl + "\t" + outputS3Key;
                
                sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(workerToManagerQueueUrl)
                    .messageBody(doneMessage)
                    .build());
                
                System.out.println("Job Done: " + outputS3Key);

            } catch (Exception e) {
                System.err.println("Worker Error: " + e.getMessage());
                e.printStackTrace();

                if (bucketName != null && analysisType != null && textUrl != null) {
                    String errorMessage = bucketName + "\t" + analysisType + "\t" + textUrl + "\t" + "ERROR: " + e.getMessage();
                    sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(workerToManagerQueueUrl)
                        .messageBody(errorMessage)
                        .build());
                }
            } finally {
                if (jobMessage != null) {
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(managerToWorkersQueueUrl)
                        .receiptHandle(jobMessage.receiptHandle())
                        .build());
                }
            }
        }
    }

    private static void initializePipelines() {
        System.out.println("Initializing pipelines (this takes time)...");
        Properties posProps = new Properties();
        posProps.setProperty("annotators", "tokenize, ssplit, pos");
        posPipeline = new StanfordCoreNLP(posProps);

        Properties conProps = new Properties();
        conProps.setProperty("annotators", "tokenize, ssplit, pos, parse");
        conProps.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
        conProps.setProperty("parse.maxtime", "120"); 
        constituencyPipeline = new StanfordCoreNLP(conProps);

        Properties depProps = new Properties();
        depProps.setProperty("annotators", "tokenize, ssplit, pos, depparse");
        depProps.setProperty("depparse.maxtime", "120"); 
        dependencyPipeline = new StanfordCoreNLP(depProps);
        System.out.println("Pipelines ready.");
    }

    private static String performAnalysis(String analysisType, String textUrl) throws IOException {
        StanfordCoreNLP pipeline;
        switch (analysisType.toUpperCase()) {
            case "POS": pipeline = posPipeline; break;
            case "CONSTITUENCY": pipeline = constituencyPipeline; break;
            case "DEPENDENCY": pipeline = dependencyPipeline; break;
            default: throw new IllegalArgumentException("Unknown analysis type: " + analysisType);
        }

        StringBuilder resultBuilder = new StringBuilder();
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(textUrl).openStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                Annotation document = new Annotation(line);
                pipeline.annotate(document);
                
                List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

                for (CoreMap sentence : sentences) {
                    switch (analysisType.toUpperCase()) {
                        case "POS":
                            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                                resultBuilder.append(token.word()).append("/").append(token.tag()).append(" ");
                            }
                            resultBuilder.append("\n");
                            break;
                        case "CONSTITUENCY":
                            resultBuilder.append(sentence.get(TreeCoreAnnotations.TreeAnnotation.class).toString()).append("\n");
                            break;
                        case "DEPENDENCY":
                            resultBuilder.append(sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class).toString()).append("\n");
                            break;
                    }
                }
            }
        }
        return resultBuilder.toString();
    }
}
