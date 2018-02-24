using System;
using System.Linq;

/// <summary>
/// CassandraCSharpDriver.3.4.0.1 http://github.com/datastax/csharp-driver
/// Install-Package CassandraCSharpDriver
/// full sample: http://academy.datastax.com/resources/getting-started-apache-cassandra-and-c-net
/// </summary>
namespace CassandraClient
{
    internal class CassandraDemo
    {
        private static readonly string _KeySpace = "\"Demo\""; // MUST HAS ""
        private static readonly string _Table = "\"DemoUser\""; // MUST HAS ""

        private static void Main(string[] args)
        {
            try
            {
                // Connect to the demo keyspace1 on our cluster running at 127.0.0.1
                var cluster = Cassandra.Cluster.Builder()
                    .AddContactPoint("127.0.0.1")
                    .WithCredentials("cassandra", "cassandrapwd")
                    .WithDefaultKeyspace(_KeySpace.Replace("\"", ""))
                    .Build();
                var session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(); // CQL: SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'Demo';

                // Check exists & create table
                var tableExists = session.Execute("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '" + _KeySpace.Replace("\"", "") + "' AND table_name = '" + _Table.Replace("\"", "") + "'").FirstOrDefault();
                if (tableExists != null && tableExists.Count() > 0 && Convert.ToInt32(tableExists[0]) == 0)
                {
                    session.Execute("CREATE TABLE " + _Table + " (email TEXT, age INT, PRIMARY KEY (email))");
                }
                // Insert sample data
                session.Execute("INSERT INTO " + _KeySpace + "." + _Table + " (email, age) values ('" + DateTime.Now.ToString("yyyy-MM-dd_HH:mm:ss.ff") + "@example.com', " + new Random((int)DateTime.Now.Ticks).Next(1, 100) + ")");

                // Show data
                var result = session
                    .Execute("SELECT email, age FROM " + _Table + " WHERE age >= 10 ALLOW FILTERING")
                    .OrderBy(r => Convert.ToInt32(r["age"]))
                    .ToDictionary(r => r["email"].ToString(), r => Convert.ToInt32(r["age"]));
                foreach (var v in result)
                {
                    Console.WriteLine(v.Key + ", " + v.Value);
                }
                Console.WriteLine("Total returning: " + result.Count());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            Console.ReadKey();
        }
    }
}
