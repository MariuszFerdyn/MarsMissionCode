using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Monitoring
{
    class Program
    {
        static string connectionString = "HostName=marsik.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=IYb+7ICxOJPhQN245DkwCFKa9gm5DC5ClSmlyV0M0iE=";
        static string monitoringEndpointName = "iothub-ehub-marsik-145869-78e734f81d";
        static EventHubClient eventHubClient;

        static void Main(string[] args)
        {
            Console.WriteLine("Monitoring. Press Enter key to exit.\n");

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, monitoringEndpointName);
            var d2cPartitions = eventHubClient.GetRuntimeInformation().PartitionIds;
            CancellationTokenSource cts = new CancellationTokenSource();
            var tasks = new List<Task>();

            foreach (string partition in d2cPartitions)
            {
                tasks.Add(ReceiveMessagesFromDeviceAsync(partition, cts.Token));
            }

            Console.ReadLine();
            Console.WriteLine("Exiting...");
            cts.Cancel();
            Task.WaitAll(tasks.ToArray());
        }

        private static async Task ReceiveMessagesFromDeviceAsync(string partition, CancellationToken ct)
        {
            var eventHubReceiver = eventHubClient.GetDefaultConsumerGroup().CreateReceiver(partition, DateTime.UtcNow);
            while (true)
            {
                if (ct.IsCancellationRequested)
                {
                    await eventHubReceiver.CloseAsync();
                    break;
                }

                EventData eventData = await eventHubReceiver.ReceiveAsync(new TimeSpan(0, 0, 10));

                if (eventData != null)
                {
                    string data = Encoding.UTF8.GetString(eventData.GetBytes());
                    Console.WriteLine("Message received. Partition: {0} Data: '{1}'", partition, data);
                }
            }
        }
    }
