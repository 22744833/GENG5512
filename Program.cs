using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MySql.Data.MySqlClient;

namespace FileUploader
{
    //author: Josh Radich
    //student No: 22744833
    //unit: GENG5512
    //description: This program was written to import file data into a MySQL database for testing 
    //             The program sets the data up in both 1 file per table and the unpivoted table format.


    class Program
    {
        private static string dataSource = "db-patrec-wordpress.cwzsb5ss938l.ap-southeast-2.rds.amazonaws.com";
        private static string db = "jradich_data";
        private static string user = "";
        private static string password = "";  //username and password have been removed 
        private static string conString = "Data Source=" + dataSource + ";Initial Catalog=" + db + "; User ID=" + user + ";Password=" + password;
        private static MySqlConnection con;
        public static MySqlConnection Con
        {
            get {
                if(con == null) //If connection to database hasn't been created yet, create it.
                {
                    con = new MySqlConnection(conString);
                }
                if(con.State != System.Data.ConnectionState.Open)
                {
                    con.Open();
                }
                return con;
            }
        }

        static void Main(string[] args)
        {
            //path where the statistical data files are kept. 
            string path = @"C:\Uni\GENG5512\data";

            //get list of files in directory
            var files = Directory.GetFiles(path);

            foreach (var file in files) //process each file 
            {
                Console.WriteLine("Processing file: " + file);
                try
                {
                    bool success= OpenFileAndCreateTable(file);  //create table for file first
                    if (success)
                    {
                        UnpivotData(file);  //if above is successful, put the data into the unpivoted tables 
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("There was an issue with " + file + " - " +  e.Message);
                }
            }

            Console.WriteLine("File Import Complete");
            Console.ReadLine();
        }

        static bool OpenFileAndCreateTable(string file)
        {
            try
            {
                var lines = File.ReadAllLines(file);    //read all lines of file 
                var titleLine = lines[0];   //get the line of column headers 
                var titleLines = titleLine.Split(',').Select(x => x.Trim());  //convert into an array 

                string fileName = Path.GetFileNameWithoutExtension(file);

                string createTable = "CREATE TABLE " + fileName + " (";   //start CREATE TABLE script 

                foreach (var col in titleLines) //create columns part  
                {
                    string val = col + " text null,";
                    createTable += val;
                }

                createTable = createTable.Substring(0, createTable.Length - 1);
                createTable += ")";
                MySqlCommand cmd = new MySqlCommand(createTable, Con);
                cmd.ExecuteNonQuery();   //create the table 

                string insertStuff = "insert into " + fileName + " values";  //create insert data script 
                foreach (var col in lines.Skip(1)) //loop over data and add to script 
                {
                    var val = col.Replace(",", "','");
                    val = "('" + val + "'),";
                    insertStuff += val;
                }
                insertStuff = insertStuff.Substring(0, insertStuff.Length - 1);
                cmd = new MySqlCommand(insertStuff, Con);
                cmd.ExecuteNonQuery();  //execute script 
                return true;
            }
            catch(Exception e)
            {
                Console.WriteLine("Problem creating table and importing data: " + e.Message);
                return false;
            }

        }

        static void UnpivotData(string file)
        {
            string tableName = Path.GetFileNameWithoutExtension(file);
            string sql = "INSERT INTO FILES(FileName) VALUES('" + tableName + "'); SELECT LAST_INSERT_ID()";
            MySqlCommand cmd = new MySqlCommand(sql, Con);
            object fileId = cmd.ExecuteScalar(); //insert file name into FILES table first 

            sql =   @"SELECT COLUMN_NAME  
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '" + tableName + "' ORDER BY ORDINAL_POSITION";  //get columns for table script 

            cmd = new MySqlCommand(sql, Con);
            MySqlDataReader reader = cmd.ExecuteReader(); //get column data 

            List<string> cols = new List<string>();
            while (reader.Read())
            {
                cols.Add(reader.GetString(0));  //save column info into a list 
            }
            reader.Close();
            

            bool first = true;
            string rowId = null;
            foreach (string col in cols)
            {
                
                object colId;
                if (first)
                {
                    rowId = col;
                    first = false;
                }
                else
                {       //loop over and insert column data and values into appropriate tables 
                    sql = "INSERT INTO COLUMNDATA(FileID,ColumnName) VALUES(" + fileId + ", '" + col + "'); SELECT LAST_INSERT_ID()";
                    cmd = new MySqlCommand(sql, Con);
                    colId = cmd.ExecuteScalar();

                    sql = "INSERT INTO VALUETABLE(RowID,ColumnID,Value) SELECT "+ rowId + "," + colId + "," + col + " FROM " + tableName ;
                    cmd = new MySqlCommand(sql, Con);
                    cmd.ExecuteNonQuery();
                }
            }

        }

    }
}
