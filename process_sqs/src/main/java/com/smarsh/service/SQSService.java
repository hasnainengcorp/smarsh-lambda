package com.smarsh.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

@Service
public class SQSService {

	@Value("${aws.queue}")
	private String queueName;

	@Value("${aws.src.bucket}")
	private String srcbucketName;
	
	
	public void receiveMessages() {

		SqsClient sqsClient = SqsClient.builder()

				.region(Region.US_EAST_1)

				.credentialsProvider(ProfileCredentialsProvider.create())

				.build();
		//added s3
		
		   ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
		    Region region = Region.US_EAST_1;
		    
		
		S3Client s3client = S3Client.builder().region(region)
	            .credentialsProvider(credentialsProvider)
	            .build();

		//end of s3

		GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder().queueName(queueName).build();

		String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

		System.out.println("\nReceive messages");

		try {

			// snippet-start:[sqs.java2.sqs_example.retrieve_messages]

			ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()

					.queueUrl(queueUrl)

					.visibilityTimeout(20)

					.maxNumberOfMessages(5)

					.build();

			List<Message> list = sqsClient.receiveMessage(receiveMessageRequest).messages();
			
			if(list.isEmpty()) {
				System.out.println("no items in the sqs to be process ");
			}

			for (Message message : list) {

				System.out.println("message body is "+message.body());
				System.out.println(message.messageAttributes().values());
				System.out.println(message.attributes());
				System.out.println(message.hasAttributes());
				System.out.println(message.receiptHandle());
				System.out.println(message.messageAttributes());
		
				//System.out.println(message.body()[0]);
						
				
				//parse object
				//S3 Object Key: 
				
				
				
				
				//
				

				// custom logic
				  GetObjectRequest getObjectRequest = GetObjectRequest.builder()
			                .bucket(srcbucketName)
			                .key(message.body())
			                .build();

			        ResponseBytes<GetObjectResponse> objectBytes = s3client.getObjectAsBytes(getObjectRequest);
			        byte[] data = objectBytes.asByteArray();

			       
			            // Unzip the data
			            try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
			                 ZipInputStream zis = new ZipInputStream(bais)) {

			                ZipEntry entry;
			                while ((entry = zis.getNextEntry()) != null) {
			                    byte[] buffer = new byte[1024];
			                    int len;
			                    ByteArrayOutputStream baos = new ByteArrayOutputStream();

			                    while ((len = zis.read(buffer)) > 0) {
			                        baos.write(buffer, 0, len);
			                    }

			                   
			                    //String newKey = message.body() + entry.getName();
			                    String newKey = entry.getName();
			                    
			                    System.out.println(newKey);

			                    // Upload the unzipped file to S3
			                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
			                            .bucket(srcbucketName)
			                            .key(newKey)
			                            .build();

			                    ByteArrayInputStream unzippedData = new ByteArrayInputStream(baos.toByteArray());
			                    s3client.putObject(putObjectRequest, RequestBody.fromInputStream(unzippedData, unzippedData.available()));
			                }
			            } catch (IOException e) {
			                e.printStackTrace();
			            }
			        
				
				//end of custom logic

			//	String fname = "zipped_files_1692228026018.zip";

				//s3service.getObjectBytes();
				
				
				

				DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()

						.queueUrl(queueUrl)

						.receiptHandle(message.receiptHandle())

						.build();

				sqsClient.deleteMessage(deleteMessageRequest);

			}

			// deleteMessages(sqsClient, queueUrl, list);

			for (Message message : list) {

				System.out.println(message.body());

			}

			for (Message message : list) {

				System.out.println(message.body());

			}

			// return sqsClient.receiveMessage(receiveMessageRequest).messages();

		} catch (SqsException e) {

			System.err.println(e.awsErrorDetails().errorMessage());

			System.exit(1);

		}

		// return null;

		// snippet-end:[sqs.java2.sqs_example.retrieve_messages]

	}
	
	//
	

	
}