package rabbit;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zoetian
 * @create 2/27/24
 */
public class Consumer {
    private static final String QUEUE_NAME = "post_queue";
    private static final int NUM_THREADS = 20;
    private static final String USER_NAME = "yuan";
    private static final String PASSWORD = "yuan";

    private static ConcurrentHashMap<Integer, List<LiftRide>> records = new ConcurrentHashMap<>();

    private static final String HOST ="34.211.202.216";
    private static final AtomicInteger count = new AtomicInteger(0);
    private static final  Region region = Region.US_WEST_2;

    private static final DynamoDbClient dbClient = DynamoDbClient.builder()
            .region(region)
            .build();
    private static final String SKIERDB_TABLE_NAME = "SkierDB";
    private static final String RESORTDB_TABLE_NAME = "ResortDB";


//private static final String HOST ="localhost";


    private static final int PORT = 5672;

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USER_NAME);
        factory.setPassword(PASSWORD);
        Connection connection = factory.newConnection();
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.submit(() -> {
                try {
                    consume(connection);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        try{
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            executorService.shutdown();
        }catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private static void consume(Connection connection) throws IOException {
        Channel channel = connection.createChannel();
        int prefetchCount = 10;
        channel.basicQos(prefetchCount);
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            try {
//                writeToMap(message);
                writeToDB(message);
            }  finally {
                count.getAndIncrement();
                System.out.println(" [x] Done");
                System.out.println("Total number of records consumed is: " + count.get());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }

    private static void writeToMap(String message) {
        Gson gson = new Gson();
        Response response = gson.fromJson(message, Response.class);
        int skierID = response.getSkierID();
        LiftRide liftRide = response.getLiftRide();
        if(!records.containsKey(skierID)) {
            records.put(skierID, new ArrayList<LiftRide>());
        }
        records.get(skierID).add(liftRide);

    }
    private static void writeToDB(String message){
        Gson gson = new Gson();
        Response response = gson.fromJson(message, Response.class);
        LiftRide liftRide = response.getLiftRide();
        int skierID = response.getSkierID();
        int resortID = response.getResortID();
        int liftID = liftRide.getLiftID();
        int time = liftRide.getTime();
        String dayID = response.getDayID();
        String seasonID = response.getSeasonID();
        putTtemToTableSkierDB(skierID,resortID,liftID,time,dayID,seasonID);
        putTtemToTableResortDB(skierID,resortID,liftID,time,dayID,seasonID);




    }

    private static void putTtemToTableSkierDB(int skierID, int resortID, int liftID, int time, String dayID, String seasonID) {
        String sortKey =  seasonID + "-" + resortID + "-" + dayID + "-" + liftID + "-" + time;
        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put("skierID", AttributeValue.builder().n(String.valueOf(skierID)).build());
        itemValues.put("seasonID", AttributeValue.builder().s(seasonID).build());
        itemValues.put("resortID", AttributeValue.builder().n(String.valueOf(resortID)).build());
        itemValues.put("dayID", AttributeValue.builder().s(dayID).build());
        itemValues.put("liftID", AttributeValue.builder().n(String.valueOf(liftID)).build());
        itemValues.put("time", AttributeValue.builder().n(String.valueOf(time)).build());
        itemValues.put("seasonID-resortID-dayID-liftID-time", AttributeValue.builder().s(
                sortKey).build());
        PutItemRequest request = PutItemRequest.builder()
                .tableName(SKIERDB_TABLE_NAME)
                .item(itemValues)
                .build();
        try {
            PutItemResponse response = dbClient.putItem(request);
            System.out.println(SKIERDB_TABLE_NAME + " was successfully updated. The request id is "
                    + response.responseMetadata().requestId());

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", SKIERDB_TABLE_NAME);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

    }

    private static void putTtemToTableResortDB(int skierID, int resortID, int liftID, int time, String dayID, String seasonID) {
        String sortKey =  dayID + "-" + seasonID + "-" + liftID + "-" + time + "-" + skierID;
        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        itemValues.put("skierID", AttributeValue.builder().n(String.valueOf(skierID)).build());
        itemValues.put("seasonID", AttributeValue.builder().s(seasonID).build());
        itemValues.put("resortID", AttributeValue.builder().n(String.valueOf(resortID)).build());
        itemValues.put("dayID", AttributeValue.builder().s(dayID).build());
        itemValues.put("liftID", AttributeValue.builder().n(String.valueOf(liftID)).build());
        itemValues.put("time", AttributeValue.builder().n(String.valueOf(time)).build());
        itemValues.put("dayID-seasonID-liftID-time-skierID", AttributeValue.builder().s(
                sortKey).build());
        PutItemRequest request = PutItemRequest.builder()
                .tableName(RESORTDB_TABLE_NAME)
                .item(itemValues)
                .build();
        try {
            PutItemResponse response = dbClient.putItem(request);
            System.out.println(RESORTDB_TABLE_NAME + " was successfully updated. The request id is "
                    + response.responseMetadata().requestId());

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", RESORTDB_TABLE_NAME);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

    }

}
