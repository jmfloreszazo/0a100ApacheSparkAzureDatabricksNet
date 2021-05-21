using Microsoft.Spark.Sql;
using System;

namespace HelloWorldSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder().AppName("HelloWorldSparkApp").GetOrCreate();
            var dataFrame = spark.Sql("select id, rand() as random_number from range(1000)");
            foreach (var row in dataFrame.Collect())
            {
                if (row[0] as int? % 2 == 0)
                {
                    Console.WriteLine($"row: {row[0]}");
                }
            }
        }
    }
}
