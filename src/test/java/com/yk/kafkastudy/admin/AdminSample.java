package com.yk.kafkastudy.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class AdminSample {

    private AdminClient client;
    private final String TOPIC_NAME = "new-test-topic";

    @Before
    public void setUp(){
        // 1. 方式一 配置文件
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.6:9092");
        AdminClient.create(properties);

        // 2. 方式二 map
        Map<String, Object> maps = new HashMap<>();
        maps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.6:9092");
        AdminClient.create(maps);

        client = AdminClient.create(properties);
    }

    @After
    public void after(){
        client.close();
    }

    @Test
    public void testCreateTopics(){
        NewTopic topic = new NewTopic("new-test-topic", 1, (byte)1);
        CreateTopicsResult result = client.createTopics(Collections.singleton(topic));

        System.out.println("==============");
        result.values().forEach(
                (k, v) -> {
                    try {
                        System.out.println(k + ": " + v.get(1, TimeUnit.MINUTES));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    @Test
    public void testListTopics() throws Exception{
        // 可修改配置，列出内部 topic
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);

        ListTopicsResult listTopics = client.listTopics();

        // 获取所有 name
        System.out.println("==============");
        Set<String> names = listTopics.names().get();
        names.forEach(System.out::println);

        // (name, internal)
        System.out.println("==============");
        Collection<TopicListing> topicListings = listTopics.listings().get();
        topicListings.forEach(System.out::println);

    }

    @Test
    public void testDelTopics() throws Exception{
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singleton(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }

    @Test
    public void testDescribes() throws Exception{
        // Topic 描述
        //test-topic:
        // (name=test-topic,
        // internal=false,
        // partitions=
        //   (partition=0,
        //   leader=192.168.1.6:9092 (id: 0 rack: null),
        //   replicas=192.168.1.6:9092 (id: 0 rack: null),
        //   isr=192.168.1.6:9092 (id: 0 rack: null)), a
        //   uthorizedOperations=[]
        //   )

        DescribeTopicsResult result = client.describeTopics(Collections.singleton("test-topic"));
        Map<String, TopicDescription> map = result.all().get();
        System.out.println("==============");
        map.forEach(
                (k, v) -> {
                    try {
                        System.out.println(k + ": " + v.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    @Test
    public void testDescribeConfigs() throws Exception{
        // 配置描述
        // ConfigResource(type=TOPIC, name='test-topic'):
        // Config(
        // entries=[
        //     ConfigEntry(
        //     name=compression.type,
        //     value=producer,
        //     source=DEFAULT_CONFIG,
        //     isSensitive=false,
        //     isReadOnly=false,
        //     synonyms=[]),
        // ConfigEntry(
        //     name=leader.replication.throttled.replicas,
        //     value=,
        //     source=DEFAULT_CONFIG,
        //     isSensitive=false,
        //     isReadOnly=false,
        //     synonyms=[]),
        // ConfigEntry(
        //     name=message.downconversion.enable,
        //     value=true,
        //     source=DEFAULT_CONFIG,
        //     isSensitive=false,
        //     isReadOnly=false,
        //     synonyms=[]),
        // ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.format.version, value=2.4-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])


        // 预留：集群时会讲
//        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "test-topic");

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "test-topic");
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> map = result.all().get();

        System.out.println("==================");
        map.forEach(
                (k, v) -> {
                    try {
                        System.out.println(k + ": " + v.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
    }

    /**
     * 修改配置信息
     */
    @Test
    public void testAlterConfig() throws Exception{

        // 组织两个参数
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "test-topic");
        Config config = new Config(
                Collections.singletonList(new ConfigEntry("preallocate", "true"))
        );
        HashMap<ConfigResource, Config> map = new HashMap<>();
        map.put(resource, config);

        AlterConfigsResult result = client.alterConfigs(map);
        result.all().get();

        // 重新查看配置信息
        DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> map2 = describeConfigsResult.all().get();
        map2.forEach((key,value) -> {
            System.out.println(key + ": " + value);
        });
    }

    /**
     * 增加 partition 数量
     * @throws Exception
     */
    @Test
    public void testIncrPatitions() throws Exception{

        Map<String, NewPartitions> map = new HashMap<>();
        NewPartitions newPartition = NewPartitions.increaseTo(2);
        map.put("test-topic", newPartition);

        CreatePartitionsResult result = client.createPartitions(map);
        Void unused = result.all().get();

    }

}
