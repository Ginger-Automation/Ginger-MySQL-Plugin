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
using Amdocs.Ginger.Plugin.Core.Reporter;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace MySQLDatabase
{
    [GingerService("MySQLService", "MySQL Database service")]
    public class MYSQLDBConnection : IDatabase
    {
        private MySqlConnection mMySqlConnection = null;
        private DbTransaction tran = null;

        private IReporter mReporter;
        

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


        [DatabaseParam("PWD")]
        
        [Default("??")]
        public string Pwd { get; set; }


        [DatabaseParam("Port")]        
        public int Port { get; set; }

        
        public string GetConnectionString()
        {
            string ConnectionString = $"Server={Server};Database={Database};Uid={Uid};Pwd={Pwd}";
            return ConnectionString;
        }

        public bool OpenConnection()
        {
            
            // string connectConnectionString = GetConnectionString(parameters);
            try
            {
                mMySqlConnection = new MySqlConnection();
                mMySqlConnection.ConnectionString = GetConnectionString();
                mMySqlConnection.Open();

                if (mMySqlConnection.State == ConnectionState.Open)
                {
                    return true;
                }
            }
            catch (Exception ex)
            {
            // FIXME check null and init mReporter of DB
                // mReporter.ToLog2(eLogLevel.ERROR, "DB connection failed, Connection String =" + connectConnectionString, ex);
                throw (ex);
            }
            return false;
        }
        public string GetConnectionString(Dictionary<string, string> parameters)
        {
            string connStr = null;
            bool res;
            res = false;

            string ConnectionString = parameters.FirstOrDefault(pair => pair.Key == "ConnectionString").Value;
            string User = parameters.FirstOrDefault(pair => pair.Key == "UserName").Value;
            string Password = parameters.FirstOrDefault(pair => pair.Key == "Password").Value;
            string TNS = parameters.FirstOrDefault(pair => pair.Key == "TNS").Value;
            string Name= parameters.FirstOrDefault(pair => pair.Key == "Name").Value;

            if (String.IsNullOrEmpty(ConnectionString) == false)
            {
                connStr = ConnectionString.Replace("{USER}", User);

                String deCryptValue = "";// EncryptionHandler.DecryptString(Password, ref res, false);
                if (res == true)
                { connStr = connStr.Replace("{PASS}", deCryptValue); }
                else
                { connStr = connStr.Replace("{PASS}", Password); }
            }
            else
            {
                String strConnString = TNS;
               
                connStr = "Data Source=" + TNS + ";User Id=" + User + ";";

                String deCryptValue = "";// EncryptionHandler.DecryptString(Password, ref res, false);

                if (res == true) { connStr = connStr + "Password=" + deCryptValue + ";"; }
                else { connStr = connStr + "Password=" + Password + ";"; }

                connStr = "Server=" + TNS + ";Database=" + Name + ";UID=" + User + ";PWD=" + deCryptValue;
            }
            return connStr;
        }

        public void CloseConnection()
        {
            try
            {
                if (mMySqlConnection != null)
                {
                    mMySqlConnection.Close();
                }
            }
            catch (Exception e)
            {
                mReporter.ToLog(eLogLevel.ERROR, "Failed to close DB Connection", e);
                throw (e);
            }
            finally
            {
                mMySqlConnection?.Dispose();
            }
        }

        public object ExecuteQuery(string Query)
        {
            MySqlCommand _cmd = new MySqlCommand
            {
                Connection = (MySqlConnection)mMySqlConnection,
                CommandText = Query
            };
            _cmd.ExecuteNonQuery();
            MySqlDataAdapter _da = new MySqlDataAdapter(_cmd);
            DataTable results = new DataTable();
            _da.Fill(results);            
            return results;
        }


        //public string GetSingleValue(string Table, string Column, string Where)
        //{
        //    string sql = $"SELECT {Column} FROM {Table} WHERE {Where}";
        //    DataTable dataTable = DBQuery(sql);
        //    return dataTable.Rows[0][0].ToString();            
        //}

        public List<string> GetTablesColumns(string table)
        {
            DbDataReader reader = null;
            List<string> rc = new List<string>();
            if (string.IsNullOrEmpty(table))
            {
                throw new ArgumentException("table name cannot be empty");
            }
            try
            {
                DbCommand command = mMySqlConnection.CreateCommand();
                // Do select with zero records
                command.CommandText = "select * from " + table + " where 1 = 0";
                command.CommandType = CommandType.Text;

                reader = command.ExecuteReader();
                // Get the schema and read the cols
                DataTable schemaTable = reader.GetSchemaTable();
                foreach (DataRow row in schemaTable.Rows)
                {
                    string ColName = (string)row[0];
                    rc.Add(ColName);
                }
            }
            catch (Exception e)
            {
                mReporter.ToLog(eLogLevel.ERROR, "", e);
                throw (e);
            }
            finally
            {
                reader.Close();
            }
            return rc;
        }

        public List<string> GetTablesList()
        {            
            List<string> rc = new List<string>();
            try
            {
                DataTable tables = mMySqlConnection.GetSchema("Tables");                
                foreach (DataRow row in tables.Rows)
                {                    
                    rc.Add((string)row[2]);
                }
            }
            catch (Exception e)
            {
                if (mReporter!=null)
                {
                    mReporter.ToLog(eLogLevel.ERROR, "Failed to get table list for DB:" + this, e);
                }                
                throw e;
            }
            return rc;
        }    
                
        public string RunUpdateCommand(string updateCmd, bool commit = true)
        {
            string result = "";
            using (DbCommand command = mMySqlConnection.CreateCommand())
            {
                try
                {
                    if (commit)
                    {
                        tran = mMySqlConnection.BeginTransaction();
                        // to Command object for a pending local transaction
                        command.Connection = mMySqlConnection;
                        command.Transaction = tran;
                    }
                    command.CommandText = updateCmd;
                    command.CommandType = CommandType.Text;

                    int rowsAffected = command.ExecuteNonQuery();
                    if (commit)
                    {
                        tran.Commit();
                    }
                    return "rowsAffected=" + rowsAffected;
                }
                catch (Exception e)
                {
                    if (mReporter != null)
                    {
                        mReporter.ToLog(eLogLevel.ERROR, "Commit failed for:" + updateCmd, e);
                    }
                    throw e;
                }
                finally
                {
                    if (tran != null)
                    {
                        tran.Rollback();
                    }

                }
            }
        }

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

        public void InitReporter(IReporter reporter)
        {
            mReporter = reporter;
        }
    }
}
