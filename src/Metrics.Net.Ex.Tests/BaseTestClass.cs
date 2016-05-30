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

        internal JObject[] GetAllCounters(string alias, string counterName)
        {
            return GetCounterFromAlias(alias, "Counter", counterName);
        }

        internal JObject[] GetAllCountersDiff(string alias, string counterName)
        {
            return GetCounterFromAlias(alias, "CounterDiff", counterName);
        }

        private JObject[] GetCounterFromAlias(string alias, string type, string counterName)
        {
            var url = "http://" + esAddress + ":" + esPort + "/" + alias + "/_search?q=Type:" + type;
            using (var client = new WebClient())
            {
                return ParseResult(client.DownloadString(url), counterName);
            }
        }

        internal JObject[] GetAllMeter(string alias, string meterName)
        {
            return GetCounterFromAlias(alias, "Meter", meterName);
        }

        internal JObject[] GetAllMeterDiff(string alias, string meterName)
        {
            return GetCounterFromAlias(alias, "MeterDiff", meterName);
        }

        internal JObject[] ParseResult(String result, String counterName)
        {
            JObject jResult = (JObject) JsonConvert.DeserializeObject(result);
            JArray hits = jResult["hits"]["hits"] as JArray;
            var allCounterRecord = hits.Select(h => h["_source"] as JObject).ToArray();
            return allCounterRecord.Where(r => r.Value<String>("Name").EndsWith(counterName)).ToArray();
        }
    }
}
