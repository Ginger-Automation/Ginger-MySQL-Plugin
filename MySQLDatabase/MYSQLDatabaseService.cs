#region License
/*
Copyright © 2014-2019 European Support Limited

Licensed under the Apache License, Version 2.0 (the "License")
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

http://www.apache.org/licenses/LICENSE-2.0 

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and 
limitations under the License. 
*/
#endregion


using Amdocs.Ginger.Plugin.Core;
using Amdocs.Ginger.Plugin.Core.DatabaseLib;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace MySQLDatabase
{
    [GingerService("MySQLDatabaseService", "MySQL Database service")]
    public class MYSQLDatabaseService : IDatabase, ISQLDatabase 
    {

        


        // [Mandatory]
        [DatabaseParam("Server")]
        [Default("127.0.0.1")]
        public string Server { get; set; }

        [DatabaseParam("Database")]
        [Default("??")]
        public string Database { get; set; }


        [DatabaseParam("Uid")]
        [Default("??")]
        public string Uid { get; set; }


        [DatabaseParam("Pwd")]

        [Default("??")]
        public string Pwd { get; set; }


        [DatabaseParam("Port")]
        public int Port { get; set; }

        private string mConnectionString;
        public string ConnectionString
        {
            get
            {
                if (!string.IsNullOrEmpty(mConnectionString))
                {
                    return mConnectionString;
                }
                else
                {
                    string ConnectionString = $"Server={Server};Database={Database};Uid={Uid};Pwd={Pwd};InitialCatalog=";
                    return ConnectionString;
                }
            }
            set
            {
                mConnectionString = value;
            }
        }


        public DbConnection GetDbConnection()
        {
            MySqlConnection MySqlConnection = new MySqlConnection();
            MySqlConnection.ConnectionString = ConnectionString;
            return MySqlConnection;
        }


        

        // Add unique MySQLDatatbase actions which doesn't exist for other DBs

        [GingerAction("Ping", "Ping the server")]
        // Special MySQL actions
        public void Ping(GingerAction GingerAction, string aa)   // !!!!!!!! aa
        {
            MySqlConnection MySqlConnection = new MySqlConnection();
            bool b = MySqlConnection.Ping();
            GingerAction.AddOutput("Ping", b);
        }

        

        //public bool OpenConnection()
        //{
        //    mDbConnection.ConnectionString = ConnectionString;
        //    mDbConnection.Open();
        //    //// string connectConnectionString = GetConnectionString(parameters);
        //    //try
        //    //{
        //    using (MySqlConnection mMySqlConnection = new MySqlConnection(ConnectionString))
        //    {
        //        mMySqlConnection.Open();
        //    }
        //        // mMySqlConnection.ConnectionString = GetConnectionString();
        //    //    mMySqlConnection.Open();

        //    //    if (mMySqlConnection.State == ConnectionState.Open)
        //    //    {
        //    //        return true;
        //    //    }
        //    //}
        //    //catch (Exception ex)
        //    //{
        //    //// FIXME check null and init mReporter of DB
        //    //    // mReporter.ToLog2(eLogLevel.ERROR, "DB connection failed, Connection String =" + connectConnectionString, ex);
        //    //    throw (ex);
        //    //}
        //    //return false;
        //    return true;
        //}
        //public string GetConnectionString(Dictionary<string, string> parameters)
        //{
        //    string connStr = null;
        //    bool res;
        //    res = false;

        //    string ConnectionString = parameters.FirstOrDefault(pair => pair.Key == "ConnectionString").Value;
        //    string User = parameters.FirstOrDefault(pair => pair.Key == "UserName").Value;
        //    string Password = parameters.FirstOrDefault(pair => pair.Key == "Password").Value;
        //    string TNS = parameters.FirstOrDefault(pair => pair.Key == "TNS").Value;
        //    string Name= parameters.FirstOrDefault(pair => pair.Key == "Name").Value;

        //    if (String.IsNullOrEmpty(ConnectionString) == false)
        //    {
        //        connStr = ConnectionString.Replace("{USER}", User);

        //        String deCryptValue = "";// EncryptionHandler.DecryptString(Password, ref res, false);
        //        if (res == true)
        //        { connStr = connStr.Replace("{PASS}", deCryptValue); }
        //        else
        //        { connStr = connStr.Replace("{PASS}", Password); }
        //    }
        //    else
        //    {
        //        String strConnString = TNS;

        //        connStr = "Data Source=" + TNS + ";User Id=" + User + ";";

        //        String deCryptValue = "";// EncryptionHandler.DecryptString(Password, ref res, false);

        //        if (res == true) { connStr = connStr + "Password=" + deCryptValue + ";"; }
        //        else { connStr = connStr + "Password=" + Password + ";"; }

        //        connStr = "Server=" + TNS + ";Database=" + Name + ";UID=" + User + ";PWD=" + deCryptValue;
        //    }
        //    return connStr;
        //}

        //public void CloseConnection()
        //{
        //    try
        //    {
        //        if (mDbConnection != null)
        //        {
        //            mDbConnection.Close();
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        mReporter.ToLog(eLogLevel.ERROR, "Failed to close DB Connection", e);
        //        throw (e);
        //    }
        //    finally
        //    {
        //        mDbConnection?.Dispose();
        //    }
        //}

        //public object ExecuteQuery(string Query)
        //{
        //    using (MySqlConnection conn = new MySqlConnection(ConnectionString))
        //    {
        //        MySqlCommand _cmd = new MySqlCommand(Query, conn);
        //        //_cmd.ExecuteNonQuery();   //  for update                
        //        MySqlDataAdapter _da = new MySqlDataAdapter(_cmd);
        //        DataTable results = new DataTable();
        //        _da.Fill(results);
        //        return results;
        //    }
        //}


        //public string GetSingleValue(string Table, string Column, string Where)
        //{
        //    string sql = $"SELECT {Column} FROM {Table} WHERE {Where}";
        //    DataTable dataTable = DBQuery(sql);
        //    return dataTable.Rows[0][0].ToString();            
        //}



            // use generic or get all columns info
        public List<string> GetTableColumns(string table)
        {
            //DbDataReader reader = null;
            //List<string> rc = new List<string>();
            //if (string.IsNullOrEmpty(table))
            //{
            //    throw new ArgumentException("table name cannot be empty");
            //}
            //try
            //{
            //    DbCommand command = mDbConnection.CreateCommand();
            //    // Do select with zero records
            //    command.CommandText = "select * from " + table + " where 1 = 0";
            //    command.CommandType = CommandType.Text;

            //    reader = command.ExecuteReader();
            //    // Get the schema and read the cols
            //    DataTable schemaTable = reader.GetSchemaTable();
            //    foreach (DataRow row in schemaTable.Rows)
            //    {
            //        string ColName = (string)row[0];
            //        rc.Add(ColName);
            //    }
            //}
            //catch (Exception e)
            //{
            //    // mReporter.ToLog(eLogLevel.ERROR, "", e);
            //    throw (e);
            //}
            //finally
            //{
            //    reader.Close();
            //}
            //return rc;
            return null;
        }

        
        public List<string> GetTablesList(DataTable TablesSchema)
        {
            List<string> tables = new List<string>();            
            foreach (DataRow row in TablesSchema.Rows)
            {
                tables.Add((string)row[2]);
            }
            return tables;
        }

        

        //public long GetRecordCount(string Query)
        //{
        //    throw new NotImplementedException();
        //}

        



        //public string RunUpdateCommand(string updateCmd, bool commit = true)
        //{
        //    string result = "";
        //    using (DbCommand command = mMySqlConnection.CreateCommand())
        //    {
        //        try
        //        {
        //            if (commit)
        //            {
        //                tran = mMySqlConnection.BeginTransaction();
        //                // to Command object for a pending local transaction
        //                command.Connection = mMySqlConnection;
        //                command.Transaction = tran;
        //            }
        //            command.CommandText = updateCmd;
        //            command.CommandType = CommandType.Text;

        //            int rowsAffected = command.ExecuteNonQuery();
        //            if (commit)
        //            {
        //                tran.Commit();
        //            }
        //            return "rowsAffected=" + rowsAffected;
        //        }
        //        catch (Exception e)
        //        {
        //            if (mReporter != null)
        //            {
        //                mReporter.ToLog(eLogLevel.ERROR, "Commit failed for:" + updateCmd, e);
        //            }
        //            throw e;
        //        }
        //        finally
        //        {
        //            if (tran != null)
        //            {
        //                tran.Rollback();
        //            }

        //        }
        //    }
        //}

        //public bool TestConnection()
        //{
        //    bool b
        //    //mMySqlConnection = new MySqlConnection();
        //    //mMySqlConnection.ConnectionString = ConnectionString;
        //    //mMySqlConnection.Open();

        //    //if (mMySqlConnection.State == ConnectionState.Open)
        //    //{
        //    //    mMySqlConnection.Close();
        //    //    return true;
        //    //}
        //    //else
        //    //{
        //    //    return false;
        //    //}

        //}

        //public void InitReporter(IReporter reporter)
        //{
        //    mReporter = reporter;
        //}
    }
}
