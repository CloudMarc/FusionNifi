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
package com.lucidworks.marc.processors.fusion;

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

import java.nio.charset.StandardCharsets;
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
import java.util.logging.Level;
import java.util.logging.Logger;

@Tags({"fusion, pipeline, ingest, solr "})
@CapabilityDescription("Fusion as a Consumer for indexing")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class FusionProcessor extends AbstractProcessor {
    private static final Logger LOGGER = Logger.getLogger(FusionProcessor.class.getName());

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
            .description("Fusion Password for basic authentication")
            .sensitive(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The original FlowFile")
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
        relationships.add(REL_SUCCESS);
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
        LOGGER.info("FusionNifi onTrigger()...");
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String fusionUrl = context.getProperty(FUSION_URL_PROPERTY).getValue();
        final String fusionUsername = context.getProperty(FUSION_USER_PROPERTY).getValue();
        final String fusionPasswd = context.getProperty(FUSION_PASSWD_PROPERTY).getValue();
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(fusionUrl);

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        final String contents = bytes.toString();

        StringEntity requestEntity = new StringEntity(
                contents,
                ContentType.APPLICATION_JSON
        );

        try {
            String encoding = Base64.getEncoder().encodeToString((fusionUsername.concat(":").concat(fusionPasswd)).getBytes(StandardCharsets.UTF_8));

            post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            post.setHeader(HttpHeaders.ACCEPT, "application/json");
            post.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);
            post.setEntity(requestEntity);
            HttpResponse response = client.execute(post);
        } catch (Exception e) {
            System.out.println(e.getCause());
        }
        session.transfer(flowFile, REL_SUCCESS);

    }
}
