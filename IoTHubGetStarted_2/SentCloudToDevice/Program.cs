using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;

namespace SentCloudToDevice
{
    class Program
    {
        static ServiceClient serviceClient;
        static string connectionString = "HostName=marsik.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=IYb+7ICxOJPhQN245DkwCFKa9gm5DC5ClSmlyV0M0iE=";
        private async static Task SendCloudToDeviceMessageAsync()
        {
            var commandMessage = new Message(Encoding.ASCII.GetBytes("Cloud to device message."));
            await serviceClient.SendAsync("myFirstDevice", commandMessage);
        }
        static void Main(string[] args)
        {
            Console.WriteLine("Send Cloud-to-Device message\n");
            serviceClient = ServiceClient.CreateFromConnectionString(connectionString);

            Console.WriteLine("Press any key to send a C2D message.");
            Console.ReadLine();
            SendCloudToDeviceMessageAsync().Wait();
            Console.ReadLine();
        }
    }
}
