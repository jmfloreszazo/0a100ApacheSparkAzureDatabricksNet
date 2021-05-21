using System;
using Microsoft.Spark.Sql;

namespace ReadEnviroCSV
{
    class Program
    {
        static void Main(string[] args)
        {

            var spark = SparkSession
                .Builder()
                .AppName("Enviro_Spark")
                .GetOrCreate();

            DataFrame dataFrame = spark.Read().Csv(@"C:\temp\ReadEnviroCSV\monitoring.csv");

            dataFrame.CreateOrReplaceTempView("Measures");

            DataFrame sqlDF = spark.Sql("SELECT AVG(Measures._c5) as AvgTemperature FROM Measures");

            sqlDF.Show();

            spark.Stop();

        }
    }
}
