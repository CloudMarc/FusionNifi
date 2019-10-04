/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lucidworks.marc.processors.sample;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Base64;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.HttpResponse;
import java.io.ByteArrayOutputStream;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.*;
import org.apache.http.entity.*;

@Tags({"fusion, pipeline, ingest, solr "})
@CapabilityDescription("Fusion as a Consumer for indexing")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FusionProcessor extends AbstractProcessor {

    public static final PropertyDescriptor FUSION_URL_PROPERTY = new PropertyDescriptor
            .Builder().name("FUSION_URL_PROPERTY")
            .displayName("Fusion URL")
            .description("Fusion URL, including collection and pipeline")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FUSION_USER_PROPERTY = new PropertyDescriptor
            .Builder().name("FUSION_USER_PROPERTY")
            .displayName("Fusion Username")
            .description("Fusion Username for basic auth")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FUSION_PASSWD_PROPERTY = new PropertyDescriptor
            .Builder().name("FUSION_PASSWD_PROPERTY")
            .displayName("Fusion Password")
            .description("Fusion Password for basic auth")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("MY_RELATIONSHIP")
            .description("Example relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(FUSION_URL_PROPERTY);
        descriptors.add(FUSION_USER_PROPERTY);
        descriptors.add(FUSION_PASSWD_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        // TODO implement
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost("http://localhost:8764/api/index-pipelines/nifi_kafka_001/collections/nifi_kafka_001/index");

        //Gson gson = new Gson();

        //List<BasicNameValuePair> arguments = new ArrayList<>();
        //arguments.add(new BasicNameValuePair("source", "Sample Producer"));
        //arguments.add(new BasicNameValuePair("node", "Codys Mac"));
        //arguments.add(new BasicNameValuePair("type", "Test Event Type"));
        //arguments.add(new BasicNameValuePair("resource", "Test Event Resource"));
        //arguments.add(new BasicNameValuePair("metric_name", "Test Event Metric Name"));
        //arguments.add(new BasicNameValuePair("event_class", "Test Event Class"));
        //arguments.add(new BasicNameValuePair("sevrity", "5"));
        //arguments.add(new BasicNameValuePair("description", "This is a test event, created by SampleProducer App usign REST API."));
	final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        final String contents = bytes.toString();
        //final String contents = "{'id': 77, 'message': 'hard coded'}";
        //#gson.put("text", contents);
	//StringEntity params = new StringEntity(json.toString());
	//StringEntity params = new StringEntity(contents);
	StringEntity requestEntity = new StringEntity(
			contents,
			ContentType.APPLICATION_JSON
			);
	//params = new StringEntity(contents);
        //arguments.add(new BasicNameValuePair("text", contents));

        try{
            //String encoding = Base64.getEncoder().encodeToString((username.concat(":").concat(password)).getBytes("UTF-8"));
            String encoding = Base64.getEncoder().encodeToString(("admin:lucidworks1").getBytes("UTF-8"));

            post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            post.setHeader(HttpHeaders.ACCEPT, "application/json");
            post.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);
            //post.setEntity(new UrlEncodedFormEntity(arguments));
            //post.setEntity(params);
            post.setEntity(requestEntity);
            HttpResponse response = client.execute(post);
        }
        catch(Exception e){
            //System.out.println(ExceptionUtils.getRootCauseMessage(e));
            System.out.println("Had problem with http post to Fusion...");
        }	
	//
	//
        //String encoding = Base64Encoder.encode("admin:lucidworks1");
	//HttpPost httpPost = new HttpPost("http://localhost:8764/api/index-pipelines/nifi_kafka_001/collections/nifi_kafka_001/index");
	//httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);
        //HttpResponse response = httpClient.execute(httpPost);
        //HttpEntity entity = response.getEntity();
    }
}
