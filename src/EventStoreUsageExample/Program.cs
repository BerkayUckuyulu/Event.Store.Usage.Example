using EventStore.Client;
using System.Text.Json;

string connectionString = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false";
var settings = EventStoreClientSettings.Create(connectionString);
var client = new EventStoreClient(settings);

Guid orderId = Guid.NewGuid();

OrderCreatedEvent orderCreatedEvent = new()
{
    OrderId = orderId,
    TotalAmount = 100
};

EventData createdEventData = new(
 eventId: Uuid.NewUuid(),
 type: orderCreatedEvent.GetType().Name,
 data: JsonSerializer.SerializeToUtf8Bytes(orderCreatedEvent)
);


OrderUpdatedEvent orderUpdatedEvent = new()
{
    OrderId = orderId,
    TotalAmount = 150
};

EventData updatedEventData = new(
 eventId: Uuid.NewUuid(),
 type: orderUpdatedEvent.GetType().Name,
 data: JsonSerializer.SerializeToUtf8Bytes(orderUpdatedEvent)
);

await client.AppendToStreamAsync(
    streamName: $"order-{orderId}-stream",
    expectedState: StreamState.Any,
    //StreamAny => stream mevcut mu değil mi kontrol etmez. direkt olarak eventi gönderir.
    //StreamExist => stream mevcutsa ekleme işlemini yapar.
    //NoStream => stream daha oluşturulmamışsa ekleme işlemini yapar. diğer durumlarda hata fırlatacaklardır.
    eventData: new[] { createdEventData, updatedEventData }
    );

var events = await client.ReadStreamAsync(
    streamName: $"order-{orderId}-stream",
    direction: Direction.Forwards,
    revision: StreamPosition.Start //Okuma baştan, sondan ya da belirli bir event numarasından    
    ).ToListAsync();


OrderCompletedEvent orderCompletedEvent = new()
{
    OrderId = orderId,
};

EventData orderCompletedEventData = new(
 eventId: Uuid.NewUuid(),
 type: orderCompletedEvent.GetType().Name,
 data: JsonSerializer.SerializeToUtf8Bytes(orderCompletedEvent)
);

await client.AppendToStreamAsync(
    streamName: $"order-{orderId}-stream",
    expectedState: StreamState.StreamExists,
    eventData: new[] { orderCompletedEventData }
    );

var eventsLast = await client.ReadStreamAsync(
    streamName: $"order-{orderId}-stream",
    direction: Direction.Forwards,
    revision: StreamPosition.Start
    ).ToListAsync();


#region Subscribe

await client.SubscribeToStreamAsync(
    streamName: $"order-9435a2fc-93a1-4205-86d7-7c20f15ce3df-stream",
    start: FromStream.Start,
    //Start => ilk başta streamdeki tüm eventleri okur.
    //End => sadece yeni eklenenleri okur.
    //After => verilen pozisyondan sonrasını okumaya başlar.
    eventAppeared: async (streamSubscription, resolvedEvent, cancellationToken) =>
    {
        var @event = JsonSerializer.Deserialize(resolvedEvent.Event.Data.ToArray(), Type.GetType(resolvedEvent.Event.EventType));
        await Console.Out.WriteLineAsync(JsonSerializer.Serialize(@event));
    },
    subscriptionDropped: (streamSubscription, subscriptionDroppedReason, exception) => Console.WriteLine("Disconnected")
    );

#endregion


Console.Read();

class OrderCreatedEvent
{
    public Guid OrderId { get; set; }
    public int TotalAmount { get; set; }
}
class OrderUpdatedEvent
{
    public Guid OrderId { get; set; }
    public int TotalAmount { get; set; }
}
class OrderCompletedEvent
{
    public Guid OrderId { get; set; }
}