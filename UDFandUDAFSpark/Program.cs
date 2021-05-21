using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using System;
using Apache.Arrow;
using Microsoft.Data.Analysis;
using Microsoft.Spark.Sql.Types;

namespace UDFandUDAFSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            // Sample UDF
            //var spark1 = SparkSession
            //    .Builder().AppName("UDFSparkApp").GetOrCreate();
            //Func<Column, Column> udfIntToString = Udf<int, string>(
            //    str => $"Id = {str}");
            //var dataFrame = spark.Sql("SELECT ID from range(1000)");
            //dataFrame.Select(udfIntToString(dataFrame["ID"])).Show();

            // Sample UDAF
            var spark = SparkSession.Builder().AppName("UDAFSparkApp").GetOrCreate();
            var dataFrame = spark.Sql(
                "SELECT 'One' as User, 'Product1' as Product, 0.99 as Cost UNION ALL SELECT 'Two', 'Product2', 1.99 UNION ALL SELECT 'One', 'Product2', 1.99 UNION ALL SELECT 'One', 'Product3', 2.99 UNION ALL SELECT 'Two', 'Product1', 0.99");
            dataFrame = dataFrame.WithColumn("Cost", dataFrame["Cost"].Cast("Float"));
            dataFrame.Show();
            var expenses = dataFrame.GroupBy("User").Apply(new StructType(new[]
            {
                new StructField("User", new StringType()),
                new StructField("TotalCost", new FloatType())
            }), TotalCost);
            expenses.PrintSchema();
            expenses.Show();
        }

        private static RecordBatch TotalCost(RecordBatch records)
        {
            var purchaseColumn = records.Column("Purchase") as StringArray;
            var costColumn = records.Column("Cost") as FloatArray;
            float totalCost = 0F;
            if (purchaseColumn != null)
                for (int i = 0; i < purchaseColumn.Length; i++)
                {
                    if (costColumn != null)
                    {
                        var cost = costColumn.GetValue(i);
                        var purchase = purchaseColumn.GetString(i);
                        if (purchase != "Product1" && cost.HasValue)
                            totalCost += cost.Value;
                    }
                }
            int returnLength = records.Length > 0 ? 1 : 0;
            return new RecordBatch(
                new Schema.Builder()
                    .Field(f => f.Name("User").DataType(Apache.Arrow.Types.StringType.Default)).Field(f =>
                        f.Name("TotalCost").DataType(Apache.Arrow.Types.FloatType.Default))
                    .Build(),
                new IArrowArray[]
                {
                    records.Column("User"),
                    new FloatArray.Builder().Append(totalCost).Build()
                }, returnLength);
        }
    }
}

