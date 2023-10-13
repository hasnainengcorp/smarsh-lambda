package com.smarsh.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

@Service
public class S3BucketStorageService {

    private Logger logger = LoggerFactory.getLogger(S3BucketStorageService.class);

    @Value("${aws.queue}")
	private String queueName;

	@Value("${aws.src.bucket}")
	private String srcbucketName;
    
    public void senddatas3tosqs() {
	    try {
	        // Initialize the S3 client
	        S3Client s3Client = S3Client.builder()
	                .region(Region.US_EAST_1) // Replace with your desired region
	                .credentialsProvider(DefaultCredentialsProvider.create())
	                .build();

	        // Initialize the SQS client
	        SqsClient sqsClient = SqsClient.builder()
	                .region(Region.US_EAST_1) // Replace with your desired region
	                .credentialsProvider(DefaultCredentialsProvider.create())
	                .build();

	        // Get a list of objects in the S3 bucket
	        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
	                .bucket(srcbucketName)
	                .build();

	        ListObjectsV2Response objectListing = s3Client.listObjectsV2(listObjectsRequest);

	        for (S3Object s3Object : objectListing.contents()) {
	            // Create a message with S3 object information and send it to the SQS queue
	        	//send msg to sqs in different format
	        	
	        	/*
	            String messageBody = "S3 Object Key: " + s3Object.key() +
	                    ", Size: " + s3Object.size() +
	                    ", Last Modified: " + s3Object.lastModified();
	                    */
	            String messageBody=s3Object.key();
	            
	            logger.info("MESSAGE BODY"+messageBody);
	            
	          //  System.out.println(messageBody);

	            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
	                    .queueUrl(getQueueUrl(sqsClient, queueName))
	                    .messageBody(messageBody)
	                    .build();

	            sqsClient.sendMessage(sendMessageRequest);
	        }
	    } catch (S3Exception | SqsException e) {
	        e.printStackTrace();
	    }
	}
    
    private String getQueueUrl(SqsClient sqsClient, String queueName) {
	    GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
	            .queueName(queueName)
	            .build();

	    return sqsClient.getQueueUrl(getQueueRequest).queueUrl();
	}
 
}
