using Amdocs.Ginger.CoreNET.Drivers.CommunicationProtocol;
using Amdocs.Ginger.Plugin.Core;
using System;

namespace MySQLDatabase
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "MySQL Database plugin";
            Console.WriteLine("Starting MySQL Database Plugin");

            using (GingerNodeStarter gingerNodeStarter = new GingerNodeStarter())
            {
                if (args.Length > 0)
                {
                    gingerNodeStarter.StartFromConfigFile(args[0]);  // file name 
                }
                else
                {                    
                    gingerNodeStarter.StartNode("MSAccess Service 1", new MYSQLDatabaseService(), SocketHelper.GetLocalHostIP(), 15001);                    
                }
                gingerNodeStarter.Listen();
            }

        }
    }
}
