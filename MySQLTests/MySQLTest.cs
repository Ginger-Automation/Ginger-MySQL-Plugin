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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySQLDatabase;
using System;
using System.Collections.Generic;
using System.Data;

namespace MySQLTests
{
 
    [TestClass]
    public class MySQLTest
    {
        public static MYSQLDBConnection db = new MYSQLDBConnection();

        [ClassInitialize]
        public static void ClassInit(TestContext context)
        {            
            db.Server = "127.0.0.1";
            db.Database = "sys";
            db.Uid = "root";
            db.Pwd = "Hello!12345";

            db.OpenConnection();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            db.CloseConnection();
        }

        [TestInitialize]
        public void TestInitialize()
        {
            
        }

        [TestCleanup]
        public void TestCleanUp()
        {
            
        }

        

        [TestMethod]
        public void GetTableList()
        {
            //Arrange                        

            //Act
            List<string> Tables = db.GetTablesList();

            //Assert
            Assert.AreEqual(4, Tables.Count);
            Assert.AreEqual("authors", Tables[1]);
            Assert.AreEqual("sys_config", Tables[2]);
            Assert.AreEqual("tutorials_tbl", Tables[3]);
        }

        [TestMethod]
        public void GetTablesColumns()
        {
            //Arrange            
            string tablename = "tbcustomers";

            //Act
            List<string> Columns = db.GetTablesColumns(tablename);

            //Assert
            Assert.AreEqual(4, Columns.Count);
            Assert.AreEqual("id", Columns[1]);
            Assert.AreEqual("name", Columns[2]);
            Assert.AreEqual("email", Columns[3]);
        }

        [TestMethod]
        public void RunUpdateCommand()
        {
            //Arrange
            string upadateCommand = "UPDATE authors SET email='aaa@aa.com' where id=3";
            
            //Act
            string result = db.RunUpdateCommand(upadateCommand, false);

            //Assert
            Assert.AreEqual(result, "1");
        }

        

        [TestMethod]
        public void ExecuteQuery()
        {
            //Arrange            

            //Act
            DataTable result = (DataTable)db.ExecuteQuery("SELECT * FROM tbcustomers");

            //Assert
            Assert.AreEqual(result.Rows.Count, 3);
        }

        
    }
}
