using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json;

record Post(int Id,string Title,string Content);
class Program
{
    public static async Task Main()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9094",
            GroupId = "newPost",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };


        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(new string[] {"addedPosts"});
            bool cancelled = false;
            while (!cancelled)
            {
                var consumeResult = consumer.Consume();
                // process message here.

                    try
                    {
                        consumer.Commit(consumeResult);
                        Console.WriteLine($"{consumeResult.Value}");
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine($"Commit error: {e.Error.Reason}");
                    }
            }
            consumer.Close();
        }
    }
}