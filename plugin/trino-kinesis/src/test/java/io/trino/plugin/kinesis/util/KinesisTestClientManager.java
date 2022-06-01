/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kinesis.util;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import io.trino.plugin.kinesis.KinesisClientProvider;

/**
 * Test implementation of KinesisClientProvider that incorporates a mock Kinesis client.
 */
public class KinesisTestClientManager
        implements KinesisClientProvider
{
    private AmazonKinesis client = new MockKinesisClient();
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonS3 amazonS3Client;

    public KinesisTestClientManager()
    {
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/java-dg-region-selection.html
        // AWS clients created by using the client constructor will not automatically determine region from the environment and will,
        // instead, use the default SDK region (USEast1).
        this.dynamoDBClient = AmazonDynamoDBClient.builder()
                .withRegion(Regions.US_EAST_1)
                .build();
        this.amazonS3Client = AmazonS3Client.builder()
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    @Override
    public AmazonKinesis getClient()
    {
        return client;
    }

    @Override
    public AmazonDynamoDB getDynamoDbClient()
    {
        return this.dynamoDBClient;
    }

    @Override
    public AmazonS3 getS3Client()
    {
        return amazonS3Client;
    }
}
