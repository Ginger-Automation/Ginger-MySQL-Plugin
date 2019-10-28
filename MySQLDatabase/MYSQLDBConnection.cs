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
using Amdocs.Ginger.Plugin.Core.Database;
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
        private DbConnection oConn = null;
        private DbTransaction tran = null;
        public Dictionary<string, string> KeyvalParamatersList = new Dictionary<string, string>();
        private IReporter mReporter;
        public string Name => throw new NotImplementedException();

        string mConnectionString;
        public string ConnectionString { get => mConnectionString; set => mConnectionString = value; }

        public bool OpenConnection(Dictionary<string, string> parameters)
        {
            KeyvalParamatersList = parameters;
            // string connectConnectionString = GetConnectionString(parameters);
            try
            {
                oConn = new MySqlConnection();
                oConn.ConnectionString = ConnectionString; // connectConnectionString;
                oConn.Open();

                if ((oConn != null) && (oConn.State == ConnectionState.Open))
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
                if (oConn != null)
                {
                    oConn.Close();
                }
            }
            catch (Exception e)
            {
                mReporter.ToLog2(eLogLevel.ERROR, "Failed to close DB Connection", e);
                throw (e);
            }
            finally
            {
                oConn?.Dispose();
            }
        }

        public DataTable DBQuery(string Query)
        {
            MySqlCommand _cmd = new MySqlCommand
            {
                Connection = (MySqlConnection)oConn,
                CommandText = Query
            };
            _cmd.ExecuteNonQuery();
            MySqlDataAdapter _da = new MySqlDataAdapter(_cmd);
            DataTable results = new DataTable();
            _da.Fill(results);            
            return results;
        }


        public int GetRecordCount(string Query)
        {
            string sql = "SELECT COUNT(1) FROM " + Query;

            String rc = null;
            DbDataReader reader = null;
            
                try
                {
                    DbCommand command = oConn.CreateCommand();
                    command.CommandText = sql;
                    command.CommandType = CommandType.Text;

                    // Retrieve the data.
                    reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        rc = reader[0].ToString();
                        break; // We read only first row = count of records
                    }
                }
                catch (Exception e)
                {
                    mReporter.ToLog2(eLogLevel.ERROR, "Failed to execute query:" + sql, e);
                    throw e;
                }
                finally
                {
                    reader.Close();
                }
            
            return Convert.ToInt32(rc);
        }

        public string GetSingleValue(string Table, string Column, string Where)
        {
            string sql = "SELECT {0} FROM {1} WHERE {2}";
            sql = String.Format(sql, Column, Table, Where);
            String rc = null;
            DbDataReader reader = null;
                try
                {
                    DbCommand command = oConn.CreateCommand();
                    command.CommandText = sql;
                    command.CommandType = CommandType.Text;

                    // Retrieve the data.
                    reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        rc = reader[0].ToString();
                        break; // We read only first row
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
                finally
                {
                    reader.Close();
                }
            
            return rc;
        }

        public List<string> GetTablesColumns(string table)
        {
            DbDataReader reader = null;
            List<string> rc = new List<string>() { "" };
            if ((oConn == null || string.IsNullOrEmpty(table)))
            {
                return rc;
            }
            try
            {
                DbCommand command = oConn.CreateCommand();
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
                mReporter.ToLog2(eLogLevel.ERROR, "", e);
                throw (e);
            }
            finally
            {
                reader.Close();
            }
            return rc;
        }

        public List<string> GetTablesList(string Name = null)
        {
            List<string> rc = new List<string>() { "" };
            try
            {
                DataTable table = oConn.GetSchema("Tables");
                string tableName = "";
                foreach (DataRow row in table.Rows)
                {
                    tableName = (string)row[2];
                    rc.Add(tableName);
                }
            }
            catch (Exception e)
            {
                mReporter.ToLog2(eLogLevel.ERROR, "Failed to get table list for DB:" + this, e);
                throw (e);
            }
            return rc;
        }    
        
        public string RunUpdateCommand(string updateCmd, bool commit = true)
        {
            string result = "";
                using (DbCommand command = oConn.CreateCommand())
                {
                    try
                    {
                        if (commit)
                        {
                            tran = oConn.BeginTransaction();
                            // to Command object for a pending local transaction
                            command.Connection = oConn;
                            command.Transaction = tran;
                        }
                        command.CommandText = updateCmd;
                        command.CommandType = CommandType.Text;

                        result = command.ExecuteNonQuery().ToString();
                        if (commit)
                        {
                            tran.Commit();
                        }
                    }
                    catch (Exception e)
                    {
                        tran.Rollback();
                        mReporter.ToLog2(eLogLevel.ERROR, "Commit failed for:" + updateCmd, e);
                        throw e;
                    }
                }
            return result;
        }

        public bool TestConnection()
        {
            oConn = new MySqlConnection();
            oConn.ConnectionString = "Server=127.0.0.1;Database=sys;Uid=root;Pwd = Hello!12345";   // !!!!!!!!!!!!!!!
            oConn.Open();

            if (oConn.State == ConnectionState.Open)
            {
                oConn.Close();
                return true;
            }
            else
            {
                return false;
            }

        }

        public void InitReporter(IReporter reporter)
        {
            mReporter = reporter;
        }
    }
}
