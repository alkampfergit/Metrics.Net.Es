using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Metrics.Net.Es
{
    internal class ElasticSearchHelper
    {

        private String _hostName;

        private Int32 _port;

        private String _indexPrefix;

        private String _indexSuffixDateTimeFormat;

        public ElasticSearchHelper(String hostname, Int32 port, String indexPrefix, String indexSuffixDateTimeFormat)
        {
            _hostName = hostname;
            if (!_hostName.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            {
                _hostName = "http://" + _hostName;
            }
            _port = port;
            _indexPrefix = indexPrefix;
            _indexSuffixDateTimeFormat = indexSuffixDateTimeFormat;
        }



        public void CreateTemplate(String indexPrefix, IEnumerable<String> aliases, String templateName)
        {
            using (var client = new WebClient())
            {
                var uri = GetUriForTemplateCreation(templateName.ToLower());
                String payload = GetIndexCreateTemplatePayload(indexPrefix, aliases);
                client.UploadString(uri, "PUT", payload);
            }
        }

        public void IndexData(IEnumerable<ElasticSearchReport.ESDocument> data)
        {
            if (data.Any() == false) return;

            using (var client = new WebClient())
            {
                var json = string.Join(string.Empty, data.Select(d => d.ToJsonString()));
                var result = client.UploadString(ElasticSearchBulkUri, json);
            }
        }

        internal String GetCurrentIndexName(DateTime timestamp)
        {
            return String.Format("{0}-{1}", _indexPrefix, timestamp.ToString(_indexSuffixDateTimeFormat));
        }

        #region Helpers

        private Uri _elasticSearchUri;

        private Uri ElasticSearchBulkUri
        {
            get
            {
                return _elasticSearchUri ?? (_elasticSearchUri =
                    new Uri(String.Format(@"{0}:{1}/_bulk", _hostName, _port)));
            }
        }

      

        #endregion

        #region Call templates

        private Uri GetUriForTemplateCreation(String templateName)
        {
            return new Uri(String.Format("{0}:{1}/_template/{2}", _hostName, _port, templateName));
        }

        private String GetIndexCreateTemplatePayload(String templatePrefix, IEnumerable<String> aliases)
        {
            var aliasesDefinition = aliases
                .Select(s => String.Format(@"""{0}"" : {{}}", s))
                .Aggregate((s1, s2) => s1 + ",\n" + s2);
            return String.Format(IndexCreateTemplateCall, templatePrefix, aliasesDefinition);
        }

        private const String IndexCreateTemplateCall = @"{{
    ""template"" : ""{0}*"",
    ""settings"" : {{
        ""number_of_shards"" : 1
    }},
    ""aliases"" : {{
        {1}
    }},
    ""mappings"" : {{
        ""Meter"" : {{
            ""_all"" : {{ ""enabled"" : false }},
            ""properties"" : {{
                ""Type"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Name"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Unit"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}}
            }}
        }},
        ""MeterDiff"" : {{
            ""_all"" : {{ ""enabled"" : false }},
            ""properties"" : {{
                ""Type"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Name"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Unit"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}}
            }}
        }},
        ""Gauge"" : {{
            ""_all"" : {{ ""enabled"" : false }},
            ""properties"" : {{
                ""Type"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Name"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Unit"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}}
            }}
        }},
        ""Counter"" : {{
            ""_all"" : {{ ""enabled"" : false }},
            ""properties"" : {{
                ""Type"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Name"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Unit"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}}
            }}
        }},
        ""CounterDiff"" : {{
            ""_all"" : {{ ""enabled"" : false }},
            ""properties"" : {{
                ""Type"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Name"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Unit"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}}
            }}
        }},
        ""Histogram"" : {{
            ""_all"" : {{ ""enabled"" : false }},
            ""properties"" : {{
                ""Type"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Name"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Unit"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}}
            }}
        }},
        ""Timer"" : {{
            ""_all"" : {{ ""enabled"" : false }},
            ""properties"" : {{
                ""Type"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Name"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}},
                ""Unit"" : {{ ""type"" : ""string"", ""index"": ""not_analyzed""}}
            }}
        }}
    }}
}}";

        #endregion
    }
}
