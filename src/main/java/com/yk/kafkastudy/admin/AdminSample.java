package com.yk.kafkastudy.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.util.*;

public class AdminSample {

    AdminClient client;

    @Before
    public void getClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.61.128:9092");

        client = AdminClient.create(properties);
    }

    @After
    public void after(){
        client.close();
    }

    @Test
    public void testCreateTopics(){
        NewTopic newTopic = new NewTopic("test-b-topic", 1, (byte)1);
        client.createTopics(Collections.singleton(newTopic));
    }

    @Test
    public void testListTopics() throws Exception {

        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = client.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        names.forEach(System.out::println);

        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.forEach(
                System.out::println
        );

    }

    @Test
    public void testDelete() throws Exception{
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singleton("test-topic"));
        Void unused = deleteTopicsResult.all().get();
        System.out.println(unused);

        ListTopicsResult listTopicsResult = client.listTopics();
        Set<String> strings = listTopicsResult.names().get();
        strings.forEach(System.out::println);
    }

    /**
     *     test-a-topic: (
     *          name=test-a-topic,
     *          internal=false,
     *          partitions=(partition=0,
     *              leader=192.168.61.128:9092 (id: 0 rack: null),
     *              replicas=192.168.61.128:9092 (id: 0 rack: null),
     *              isr=192.168.61.128:9092 (id: 0 rack: null)),
     *          authorizedOperations=[]
     *     )
     */
    @Test
    public void testDescribe() throws Exception{
        DescribeTopicsResult describeTopicsResult = client.describeTopics(Collections.singleton("test-a-topic"));
        Map<String, TopicDescription> map = describeTopicsResult.all().get();

        map.forEach((key,value) -> {
            System.out.println(key + ": " + value);
        });
    }

    @Test
    public void testDescribeConfig() throws Exception{
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC,"test-a-topic");
        DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> map = describeConfigsResult.all().get();

        map.forEach((key,value) -> {
            System.out.println(key + ": " + value);
        });
    }

    @Test
    public void testAlterConfig() throws Exception{

        Map<ConfigResource, Config> map = new HashMap<>();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "test-a-topic");
        Config config = new Config(Collections.singleton(new ConfigEntry("preallocate","true")));

        map.put(resource, config);

        AlterConfigsResult alterConfigsResult = client.alterConfigs(map);
        Void unused = alterConfigsResult.all().get();

        DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> map2 = describeConfigsResult.all().get();

        map2.forEach((key,value) -> {
            System.out.println(key + ": " + value);
        });

    }

    @Test
    public void testIncrPartitions() throws Exception{
        Map<String,NewPartitions> map = new HashMap<>();

        NewPartitions partitions = NewPartitions.increaseTo(2);
        map.put("test-a-topic",partitions);

        CreatePartitionsResult result = client.createPartitions(map);
        result.all().get();
    }

}
