using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.SNSEvents;
using ReviewBooking_Sidecar.Models;

[assembly:LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
namespace ReviewBooking_Sidecar;

public class BookingStatusUpdater
{
    public async Task UpdateBooking(SNSEvent snsEvent, ILambdaContext context)
    {
        
        var dynamoDbClient = new AmazonDynamoDBClient();
        var table = Table.LoadTable(dynamoDbClient, "Booking");


        foreach (var snsEventRecord in snsEvent.Records)
        {
            var eventBody = JsonSerializer.Deserialize<ReviewBookingEvent>(snsEventRecord.Sns.Message);
            if (eventBody == null)
            {
                context.Logger.LogError("SNS Event did not return a valid event body");
                continue;
            }

            var queryResult = await table.Query(new QueryOperationConfig
            {
                Filter = new QueryFilter("Id", QueryOperator.Equal, eventBody.BookingId),
                IndexName = "Id-index"
            }).GetRemainingAsync();

            
            var document = queryResult.FirstOrDefault();
            if (document != null)
            {
                document["Status"] = eventBody.Status;
                await table.UpdateItemAsync(document, document["UserId"].ToString(),eventBody.BookingId);
            }
            else
            {
                context.Logger.LogError($"A booking with ID of {eventBody.BookingId} was not found.");
            }
 
        }
    }
}