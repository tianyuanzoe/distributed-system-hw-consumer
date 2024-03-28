package dynamodb;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * @author zoetian
 * @create 3/17/24
 */
public class DynamoDBItemCount {
    private static final String SKIERDB_TABLE_NAME = "SkierDB";
    private static final String RESORTDB_TABLE_NAME = "ResortDB";

    public static void main(String[] args) {
        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(Region.US_WEST_2)
                .build();

        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(SKIERDB_TABLE_NAME)
                .select("COUNT")
                .build();

        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

        int itemCount = queryResponse.count();
        System.out.println("Total items in the table: " + itemCount);

        dynamoDbClient.close();
    }
}

