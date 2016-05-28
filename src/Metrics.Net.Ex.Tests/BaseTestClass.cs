using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Metrics.Net.Ex.Tests
{
    public class BaseTestClass
    {
        protected string esAddress;
        protected Int32 esPort;

        protected void DeleteIndexesByPrefix(String indexPrefix)
        {
            var url = "http://" + esAddress + ":" + esPort + "/" + indexPrefix + "*";
            using (var client = new HttpClient())
            {
                client.DeleteAsync(url).Wait();
            }
        }

        internal void WaitForRecord()
        {
            throw new NotImplementedException();
        }

        internal JObject[] GetAllCounters(string alias)
        {
            return GetCounterFromAlias(alias, "Counter");
        }

        internal JObject[] GetAllCountersDiff(string alias)
        {
            return GetCounterFromAlias(alias, "CounterDiff");
        }

        private JObject[] GetCounterFromAlias(string alias, string type)
        {
            var url = "http://" + esAddress + ":" + esPort + "/" + alias + "/_search?q=Type:" + type;
            using (var client = new WebClient())
            {
                return ParseResult(client.DownloadString(url));
            }
        }

        internal JObject[] ParseResult(String result)
        {
            JObject jResult = (JObject) JsonConvert.DeserializeObject(result);
            JArray hits = jResult["hits"]["hits"] as JArray;
            return hits.Select(h => h["_source"] as JObject).ToArray();
        }
    }
}
