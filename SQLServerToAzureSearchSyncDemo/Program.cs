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
using System.Net;
using System.Net.Http;
using System.Threading;

namespace SQLServerToAzureSearchSyncDemo
{
    class Program
    {
        private static Uri _serviceUri;
        private static HttpClient _httpClient;
        private static long _lastVersion;
        private static string _indexName = "catalog";

        static void Main(string[] args)
        {
            Console.WriteLine("Sync Processing Started...\n");
            try
            {
                _serviceUri = new Uri("https://" + ConfigurationManager.AppSettings["SearchServiceName"] + ".search.windows.net");
                _httpClient = new HttpClient();
                System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
                // Get the search service connection information from the App.config
                _httpClient.DefaultRequestHeaders.Add("api-key", ConfigurationManager.AppSettings["SearchServiceApiKey"]);
            }
            catch (Exception)
            {
                Console.WriteLine("Error building the Azure Search service URL.  Did you remember to update the app.config?\n");
                return;
            }
            // TODO: For this demo _lastVersion will be set to -1 each time the worker role is launched
            //       You will most likely want to store the _lastVersion in a table so that when it is launched 
            //       (or when VM is restarted) it can get this value to pick up where it left off and so a full sync is not executed
            _lastVersion = -1;

            // This will create a database called AzureSearchSyncTest - if one already exists it will delete and re-create it
            Console.WriteLine("Creating SQL Server database with Products table...\n");
            if (ChangeEnumerator.ExecuteSQLScript(@".\sql\create_table.sql", ConfigurationManager.AppSettings["MasterDBSqlConnectionString"]) == false)
            {
                Console.WriteLine("Error creating database\n");
                return;
            }

            // This will add Change Tracking for the Products table
            Console.WriteLine("Enabling change tracking for Products table...\n");
            if (ChangeEnumerator.ExecuteSQLScript(@".\sql\add_change_tracking.sql", ConfigurationManager.AppSettings["SourceSqlConnectionString"]) == false)
            {
                Console.WriteLine("Error adding change to database\n");
                return;
            }

            // Check if index exits - If needed can call DeleteCatalogIndex() to delete the index
            if (!CatalogIndexExists())
            {
                Console.WriteLine("Creating index...\n");
                CreateCatalogIndex();
            }

            while (true)
            {
                // Query to get the initial set of data for first sync
                ChangeEnumerator changeEnumerator = new ChangeEnumerator(
                    ConfigurationManager.AppSettings["SourceSqlConnectionString"],
                    "SELECT CONVERT(NVARCHAR(32), ProductID) AS ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, SellStartDate, SellEndDate, DiscontinuedDate, CategoryName, ModelName, Description FROM Products",
                    "Version");

                // Get all the data and load into Azure Search
                ChangeSet changes = changeEnumerator.ComputeChangeSet(_lastVersion);

                if (ApplyChanges(changes))
                {
                    // Set the Change Tracking Version only if data application was successful
                    _lastVersion = changes.Version;
                    Console.WriteLine("Sync Complete, waiting 5 seconds...");
                }
                else
                {
                    //If it is not successful you may want to add logging or alerting                
                    Console.WriteLine("Data sync failed, will try again in 5 seconds...");
                }

                Thread.Sleep(5000);
            }
        }

        private static bool ApplyChanges(ChangeSet changes)
        {
            // first apply the changes and if we succeed then record the new version that 
            // we'll use as starting point next time
            // pull contents from the changeset and upload them to Azure Search in batches of up to 1000 documents each
            var indexOperations = new List<Dictionary<string, object>>();
            bool result = true;
            foreach (var change in changes.Changes)
            {
                change["@search.action"] = "mergeOrUpload"; // action can be upload, merge, mergeOrUpload or delete

                indexOperations.Add(change);

                if (indexOperations.Count > 999)
                {
                    Console.WriteLine("Uploading {0} changes...", indexOperations.Count.ToString());

                    if (IndexCatalogBatch(indexOperations) == false)
                        result = false;
                    indexOperations.Clear();
                }
            }

            if (indexOperations.Count > 0)
            {
                Console.WriteLine("Uploading {0} changes...", indexOperations.Count.ToString());
                if (IndexCatalogBatch(indexOperations) == false)
                    result = false;
            }
            return result;
        }

        private static bool CatalogIndexExists()
        {
            Uri uri = new Uri(_serviceUri, "/indexes/" + _indexName);
            HttpResponseMessage response = AzureSearchHelper.SendRequest(_httpClient, HttpMethod.Get, uri);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
            response.EnsureSuccessStatusCode();
            return true;
        }

        private static bool DeleteCatalogIndex()
        {
            Uri uri = new Uri(_serviceUri, "/indexes/" + _indexName);
            HttpResponseMessage response = AzureSearchHelper.SendRequest(_httpClient, HttpMethod.Delete, uri);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
            response.EnsureSuccessStatusCode();
            return true;
        }

        private static void CreateCatalogIndex()
        {
            var definition = new
            {
                Name = _indexName,
                Fields = new[]
                {
                    new { Name = "productID",        Type = "Edm.String",         Key = true,  Searchable = false, Filterable = false, Sortable = false, Facetable = false, Retrievable = true,  Suggestions = false },
                    new { Name = "name",             Type = "Edm.String",         Key = false, Searchable = true,  Filterable = false, Sortable = true,  Facetable = false, Retrievable = true,  Suggestions = true  },
                    new { Name = "productNumber",    Type = "Edm.String",         Key = false, Searchable = true,  Filterable = false, Sortable = false, Facetable = false, Retrievable = true,  Suggestions = true  },
                    new { Name = "color",            Type = "Edm.String",         Key = false, Searchable = true,  Filterable = true,  Sortable = true,  Facetable = true,  Retrievable = true,  Suggestions = false },
                    new { Name = "standardCost",     Type = "Edm.Double",         Key = false, Searchable = false, Filterable = false, Sortable = false, Facetable = false, Retrievable = true,  Suggestions = false },
                    new { Name = "listPrice",        Type = "Edm.Double",         Key = false, Searchable = false, Filterable = true,  Sortable = true,  Facetable = true,  Retrievable = true, Suggestions = false },
                    new { Name = "size",             Type = "Edm.String",         Key = false, Searchable = true,  Filterable = true,  Sortable = true,  Facetable = true,  Retrievable = true,  Suggestions = false },
                    new { Name = "weight",           Type = "Edm.Double",         Key = false, Searchable = false, Filterable = true,  Sortable = false, Facetable = true,  Retrievable = true,  Suggestions = false },
                    new { Name = "sellStartDate",    Type = "Edm.DateTimeOffset", Key = false, Searchable = false, Filterable = true,  Sortable = false, Facetable = false, Retrievable = false, Suggestions = false },
                    new { Name = "sellEndDate",      Type = "Edm.DateTimeOffset", Key = false, Searchable = false, Filterable = true,  Sortable = false, Facetable = false, Retrievable = false, Suggestions = false },
                    new { Name = "discontinuedDate", Type = "Edm.DateTimeOffset", Key = false, Searchable = false, Filterable = true,  Sortable = false, Facetable = false, Retrievable = true,  Suggestions = false },
                    new { Name = "categoryName",     Type = "Edm.String",         Key = false, Searchable = true,  Filterable = true,  Sortable = false, Facetable = true,  Retrievable = true,  Suggestions = true  },
                    new { Name = "modelName",        Type = "Edm.String",         Key = false, Searchable = true,  Filterable = true,  Sortable = false, Facetable = true,  Retrievable = true,  Suggestions = true  },
                    new { Name = "description",      Type = "Edm.String",         Key = false, Searchable = true,  Filterable = true,  Sortable = false, Facetable = false, Retrievable = true,  Suggestions = false }
                }
            };

            Uri uri = new Uri(_serviceUri, "/indexes");
            string json = AzureSearchHelper.SerializeJson(definition);
            HttpResponseMessage response = AzureSearchHelper.SendRequest(_httpClient, HttpMethod.Post, uri, json);
            response.EnsureSuccessStatusCode();
        }

        private static bool IndexCatalogBatch(List<Dictionary<string, object>> changes)
        {
            try
            {
                var batch = new
                {
                    value = changes
                };

                Uri uri = new Uri(_serviceUri, "/indexes/catalog/docs/index");
                string json = AzureSearchHelper.SerializeJson(batch);
                HttpResponseMessage response = AzureSearchHelper.SendRequest(_httpClient, HttpMethod.Post, uri, json);
                response.EnsureSuccessStatusCode();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
