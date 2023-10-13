package com.smarsh.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smarsh.service.S3BucketStorageService;
import com.smarsh.service.SQSService;

@RestController
@RequestMapping("/awstest")
public class S3BucketStorageController {

    @Autowired
    private SQSService service;
    
    /*
    //send s3 bucket name to sqs queue
    
    @GetMapping(value = "/s3tosqs")
    public void senddatas3tosqs() {
    	 service.senddatas3tosqs();
    }
    
*/
    
    //get message from sqs and extract file in s3
    
    @GetMapping(value="/getmessage")
    public void getMessage() {
    	service.receiveMessages();
    }
   
    
  
}