//Copyright (c) 2014 Microsoft

//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), 
//to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
//WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SQLServerToAzureSearchSyncDemo
{
    class ChangeEnumerator
    {
        private static string _connectionString;
        private static string _query;
        private static string _versionColumnName;

        static ChangeEnumerator()
        {
            // We use this so that |DataDirectory| in connection strings maps to the local directory
            // if the environment didn't set a data directory yet
            if (AppDomain.CurrentDomain.GetData("DataDirectory") == null)
            {
                AppDomain.CurrentDomain.SetData("DataDirectory", AppDomain.CurrentDomain.BaseDirectory);
            }
        }

        public ChangeEnumerator(string connectionString, string query, string versionColumnName)
        {
            _connectionString = connectionString;
            _query = query;
            _versionColumnName = versionColumnName;
        }

        public static bool ExecuteSQLScript(string fileName, string connectionString)
        {
            // Execute the specified SQL script
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                con.Open();
                try
                {
                    string script = File.ReadAllText(fileName);

                    // split script on GO command
                    IEnumerable<string> commandStrings = Regex.Split(script, @"^\s*GO\s*$",
                                             RegexOptions.Multiline | RegexOptions.IgnoreCase);

                    foreach (string commandString in commandStrings)
                    {
                        if (commandString.Trim() != "")
                        {
                            using (var command = new SqlCommand(commandString, con))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
                catch
                {
                    // If db creation fails, throw
                    throw;
                }
            }
            return true;

        }

        public ChangeSet ComputeChangeSet(long lastVersion)
        {
            SqlConnection con = new SqlConnection(_connectionString);
            con.Open();

            try
            {
                // Compute a new version first to make sure we don't lose any updates
                long version = GetChangeSetVersion(con);
                
                IEnumerable<Dictionary<string, object>> changes = EnumerateUpdatedDocuments(con, lastVersion);

                return new ChangeSet { Version = version, Changes = changes };
            }
            catch
            {
                // In non-error paths the data reader will auto-close the connection, but if 
                // we find an error let's close it here so we don't leak it
                con.Close();
                throw;
            }
        }

        private Int64 GetChangeSetVersion(SqlConnection con)
        {
            SqlCommand cmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", con);
            return Convert.ToInt64(cmd.ExecuteScalar());
        }

        private IEnumerable<Dictionary<string, object>> EnumerateUpdatedDocuments(SqlConnection con, Int64 lastVersion)
        {
            SqlCommand cmd;
            if (lastVersion == -1)
                cmd = new SqlCommand(_query, con);
            else
            {
                string sqlCmd = "SELECT CONVERT(NVARCHAR(32), CT.ProductID) AS ProductID, P.[Name], P.[ProductNumber], P.[Color], P.[StandardCost], P.[ListPrice], P.[Size], P.[Weight], P.[SellStartDate], ";
                sqlCmd += "P.[SellEndDate], P.[DiscontinuedDate], P.[CategoryName], P.[ModelName], P.[Description] ";
                sqlCmd += "FROM Products AS P ";
                sqlCmd += "RIGHT OUTER JOIN CHANGETABLE(CHANGES dbo.Products, @version) AS CT ";
                sqlCmd += "ON ";
                sqlCmd += "P.ProductID = CT.ProductID ";
                sqlCmd += "and (CT.SYS_CHANGE_OPERATION = 'U'  ";
                sqlCmd += "or CT.SYS_CHANGE_OPERATION = 'I') ";
                cmd = new SqlCommand(sqlCmd, con);
                cmd.Parameters.Add(new SqlParameter("@version", lastVersion));
            }

            using (SqlDataReader reader = cmd.ExecuteReader(System.Data.CommandBehavior.CloseConnection))
            {
                while (reader.Read())
                {
                    Dictionary<string, object> row = new Dictionary<string, object>();

                    for (int i = 0; i < reader.VisibleFieldCount; i++)
                    {
                        object value = reader.GetValue(i);
                        row[reader.GetName(i)] = value is DBNull ? null : value;
                    }

                    // Yield rows as we get them and avoid buffering them so we can easily handle
                    // large datasets without memory issues
                    yield return row;
                }
            }
        }

    }
}
