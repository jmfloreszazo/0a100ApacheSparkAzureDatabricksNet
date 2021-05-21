using Microsoft.Spark.Sql;
using System;

namespace APIsReadandFilter
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder().AppName("APIsReadandFilterApp").GetOrCreate();
            var reader = spark.Read().Format("csv").Option("header", true).Option("sep", ",");
            var dataFrame = reader.Load("monitoringoffice.csv");
            dataFrame.Write().PartitionBy("PartitionKey").Csv("output.csv");
        }
    }
}



