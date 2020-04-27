using System.Text.RegularExpressions;
using System.Data;
using System.IO;
using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
namespace ETL
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                //Execute one file after one 

                string filePath = "F:\\New folder\\ETL\\ETL\\Inputs\\2Col.csv";
                //string filePath = "F:\\New folder\\ETL\\ETL\\Inputs\\3Col.csv";
                //string filePath = "F:\\New folder\\ETL\\ETL\\Inputs\\4Col.csv";


                if (IsDBTableExists("Destination"))
                {
                    List<string> sourceColNames = GetColumnsList(filePath);
                    List<string> destColNames = DestinationTableColumns("Destination");

                    //Assuming Old columns stay as it is and new column will added to table
                    if (sourceColNames.Count != destColNames.Count)
                    {
                        var newColNames = sourceColNames.Except(destColNames).ToList();
                        if (AddNewColumnstoDBTable(newColNames, "Destination"))
                        {
                            DataTable destTable = ConvertCSVtoDataTable(filePath);
                            TransferDataToDB(destTable, "Destination");
                            Console.WriteLine("Data transfered to database successfully..");
                        }
                        else
                            Console.WriteLine("Adding new columns to database table failed..");
                    }

                }
                else
                {
                    List<string> sourceColNames = GetColumnsList(filePath);
                    if (CreateDestinationTable(sourceColNames))
                    {
                        DataTable destTable = ConvertCSVtoDataTable(filePath);
                        TransferDataToDB(destTable, "Destination");
                        Console.WriteLine("Data transfered to database successfully..");
                    }
                    else
                    {
                        Console.WriteLine("Creation of table in database is failed..");
                    }
                }
                Console.WriteLine("Process success." );
            }
            catch(Exception Ex)
            {
                Console.WriteLine("Process failed." + Ex.Message.ToString());
            }
 
        }

        public static bool AddNewColumnstoDBTable(List<string> newColNames,string tblName)
        {
            try
            {
                var colstructure = string.Empty;
                foreach (var col in newColNames)
                {
                    colstructure = colstructure + col + " int,";
                }
                colstructure = " ADD " + colstructure.Substring(0, colstructure.Length - 1);
                SqlConnection sqlConnection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=\"F:\\New folder\\ETL\\ETL\\ETLDB.mdf\";Integrated Security=True");
                string query =
                @"ALTER TABLE dbo.Destination " +
                colstructure;

                SqlCommand cmd = new SqlCommand(query, sqlConnection);
                sqlConnection.Open();
                cmd.ExecuteNonQuery();
                return true;
            }
            catch (SqlException e)
            {
                return false;
            }
        }
        public static List<string> DestinationTableColumns(string tblName)
        {
            List<string> columns = new List<string>();
            SqlConnection sqlConnection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=\"F:\\New folder\\ETL\\ETL\\ETLDB.mdf\";Integrated Security=True");
            sqlConnection.Open();
            SqlCommand sqlCommand = new SqlCommand("SELECT TOP 0 *  FROM " + tblName, sqlConnection);
            using (var reader = sqlCommand.ExecuteReader())
            {
                reader.Read();
                var table = reader.GetSchemaTable();
                foreach(DataRow row in table.Rows)
                {
                    columns.Add(row[0].ToString());
                }

                //foreach (DataColumn column in table.Columns)
                //{
                //    columns.Add(column.ColumnName);
                //}
            }
            return columns;
        }

        public static bool IsDBTableExists(string tblName)
        {

            using (SqlConnection sqlConnection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=\"F:\\New folder\\ETL\\ETL\\ETLDB.mdf\";Integrated Security=True"))
            {

                sqlConnection.Open();

                    DataTable destTable = sqlConnection.GetSchema("TABLES",
                                   new string[] { null,null, tblName });

                    return destTable.Rows.Count > 0;
                
            }
                                    
        }

        public static bool CreateDestinationTable(List<string> colNames)
        {
            try
            {
                var colstructure = string.Empty;
                foreach (var col in colNames)
                {
                    colstructure = colstructure + col + " int,";
                }

                SqlConnection sqlConnection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=\"F:\\New folder\\ETL\\ETL\\ETLDB.mdf\";Integrated Security=True");
                string query =
                @"CREATE TABLE dbo.Destination
                (" +
                        colstructure

                + ");";

                SqlCommand cmd = new SqlCommand(query, sqlConnection);
                sqlConnection.Open();
                cmd.ExecuteNonQuery();
                return true;
            }
            catch (SqlException e)
            {
                return false;
            }

           
        }

        public static bool TransferDataToDB(DataTable dtSource, string tblName)
        {
            try
            {
                SqlConnection sqlConnection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=\"F:\\New folder\\ETL\\ETL\\ETLDB.mdf\";Integrated Security=True");
                sqlConnection.Open();
                using (var bulkCopy = new SqlBulkCopy(sqlConnection.ConnectionString, SqlBulkCopyOptions.KeepIdentity))
                {
                    // my DataTable column names match my SQL Column names, so I simply made this loop. However if your column names don't match, just pass in which datatable name matches the SQL column name in Column Mappings
                    foreach (DataColumn col in dtSource.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }

                    bulkCopy.BulkCopyTimeout = 600;
                    bulkCopy.DestinationTableName = tblName;
                    bulkCopy.WriteToServer(dtSource);
                }
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public static List<String> GetColumnsList(string strFilePath)
        {

            StreamReader sr = new StreamReader(strFilePath);
            string[] headers = sr.ReadLine().Split(',');
            List<string> columns = new List<string>();
            foreach (string header in headers)
            {
                columns.Add(header);
            }

            return columns;
        }


        public static DataTable ConvertCSVtoDataTable(string strFilePath)
        {
            StreamReader sr = new StreamReader(strFilePath);
            string[] headers = sr.ReadLine().Split(',');
            DataTable dt = new DataTable();
            foreach (string header in headers)
            {
                dt.Columns.Add(header);
            }
            while (!sr.EndOfStream)
            {
                string[] rows = Regex.Split(sr.ReadLine(), ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                DataRow dr = dt.NewRow();
                for (int i = 0; i < headers.Length; i++)
                {
                    dr[i] = rows[i];
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }
    }
}
