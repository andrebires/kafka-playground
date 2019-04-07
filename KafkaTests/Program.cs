using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaTests
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string topic;

            do
            {
                Console.Write("Enter the topic name: ");
                topic = Console.ReadLine();
            } while (string.IsNullOrWhiteSpace(topic));

            int groups;
            
            do
            {
                Console.Write("Enter the number of consumer groups: ");
                int.TryParse(Console.ReadLine(), out groups);
            } while (groups == 0);

            
            int consumerPerGroups;
            
            do
            {
                Console.Write("Enter the number of consumer per group: ");
                int.TryParse(Console.ReadLine(), out consumerPerGroups);
            } while (consumerPerGroups == 0);
            

            using (var cts = new CancellationTokenSource())
            {
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                var produceTask = ProduceAsync(topic, cts.Token);
                var consumersTask =
                    Task.WhenAll(
                        Enumerable
                            .Range(0, groups)
                            .Select(groupId => Enumerable.Range(0, consumerPerGroups)
                                .Select(consumerId => ConsumeAsync($"group-{groupId}", topic, consumerId, cts.Token)))
                            .SelectMany(i => i));                        
                
                await Task.WhenAll(
                    produceTask,
                    consumersTask);
            }
        }

        private static async Task ProduceAsync(string topic, CancellationToken cancellationToken)
        {           
            await Task.Yield();
            
            Console.WriteLine($"Starting producer on topic {topic}");
            
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:29092"
            };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    Console.Write("Message: ");

                    var message = Console.ReadLine();

                    int times;
                    
                    do
                    {
                        Console.Write("Times to repeat: ");
                        int.TryParse(Console.ReadLine(), out times);
                    } while (times == 0);

                    var produceTasks = new List<Task>();
                    
                    while (times-- > 0)
                    {
                                                  
                        var produceTask = producer.ProduceAsync(
                            topic,
                            new Message<Null, string>
                            {
                                Value = message
                            },
                            cancellationToken)
                            .ContinueWith(r =>
                            {
                                if (r.Exception != null)
                                {
                                    if (r.Exception.InnerException is ProduceException<Null, string> e)
                                    {
                                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                                    }
                                    else
                                    {
                                        Console.WriteLine(r.Exception.InnerException);

                                        
                                    }

                                }
                                
                                Console.WriteLine(
                                    $"Delivered '{r.Result.Value}' to '{r.Result.TopicPartitionOffset}'");                                    
                            },
                            cancellationToken);

                        produceTasks.Add(produceTask);                                                                                                
                    }                    

                    await Task.WhenAll(produceTasks);
                }
                
                Console.WriteLine("Producer stopped");
            }
        }

        private static async Task ConsumeAsync(string groupId, string topic, int consumerId, CancellationToken cancellationToken)
        {
            await Task.Yield();
            
            Console.WriteLine($"Starting consumer {consumerId} for group {groupId} on topic {topic}");
            
            var conf = new ConsumerConfig
            {                                 
                GroupId = groupId,
                BootstrapServers = "localhost:29092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                consumer.Subscribe(topic);                

                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {                                                        
                            var consumeResult = consumer.Consume(cancellationToken);
                            Console.WriteLine($"Consumed message '{consumeResult.Value}' at: '{consumeResult.TopicPartitionOffset} on group {groupId}' by consumer {consumerId}.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
            
            Console.WriteLine($"Consumer {consumerId} on group {groupId} stopped");
        }
    }
}