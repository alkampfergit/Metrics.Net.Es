﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Metrics.Json;
using Metrics.MetricData;
using Metrics.Reporters;
using Metrics.Utils;
using Metrics;
using System.Text;

namespace Metrics.Net.Es
{
    public class ElasticSearchReport : BaseReport, IDisposable
    {
        private String _hostName;
        private String _indexPrefix = "MetricsReports";
        private Int32 _port = 9200;
        private String _indexSuffixDateTimeFormat = "yyyyMM";

        private List<String> _aliases = new List<string>();

        private ElasticSearchHelper _esHelper;
        internal class ESDocument
        {
            public string Index { get; set; }
            public string Type { get; set; }
            public JsonObject Object { get; set; }
            public string ToJsonString()
            {
                var meta = string.Format("{{ \"index\" : {{ \"_index\" : \"{0}\", \"_type\" : \"{1}\"}} }}", this.Index, this.Type);
                return meta + Environment.NewLine + this.Object.AsJson(false) + Environment.NewLine;
            }
        }

        private List<ESDocument> data = null;

        public ElasticSearchReport()
        {
        }

        #region Internal functions

        internal String Validate()
        {
            StringBuilder errors = new StringBuilder();
            if (String.IsNullOrEmpty(_hostName))
                errors.AppendLine("HostName is not configured");
            if (String.IsNullOrEmpty(_indexPrefix))
                errors.AppendLine("Index prefix is not configured properly");
            if (_aliases.Count == 0)
                errors.AppendLine("at least one alias should be specified");

            return errors.ToString();
        }

        internal void Init()
        {
            _esHelper = new ElasticSearchHelper(_hostName, _port, _indexPrefix, _indexSuffixDateTimeFormat);
            _esHelper.CreateTemplate(_indexPrefix, _aliases, "metricsDotNetTemplate");
        }

        #endregion  

        #region Fluent configuration

        public ElasticSearchReport HostName(String hostName)
        {
            _hostName = hostName;
            return this;
        }

        public ElasticSearchReport HostPort(Int32 port)
        {
            _port = port;
            return this;
        }

        public ElasticSearchReport WithIndexPrefix(String indexPrefix)
        {
            _indexPrefix = indexPrefix;
            return this;
        }
        public ElasticSearchReport AddAlias(String indexAlias)
        {
            _aliases.Add(indexAlias);
            return this;
        }

        public ElasticSearchReport SetIndexSuffixDateTimeFormat(String format)
        {
            _indexSuffixDateTimeFormat = format;
            return this;
        }

        #endregion

        #region Reporting 

        protected override void StartReport(string contextName)
        {
            this.data = new List<ESDocument>();
            base.StartReport(contextName);
        }

        protected override void EndReport(string contextName)
        {
            base.EndReport(contextName);
            if (_isDisposed) return;
            _esHelper.IndexData(data);
        }

        private void Pack(string type, string name, Unit unit, MetricTags tags, IEnumerable<JsonProperty> properties)
        {
            this.data.Add(new ESDocument
            {
                Index = _esHelper.GetCurrentIndexName(CurrentContextTimestamp),
                Type = type,
                Object = new JsonObject(new[] {
                         new JsonProperty("Timestamp", Clock.FormatTimestamp(this.CurrentContextTimestamp)),
                         new JsonProperty("Type",type),
                         new JsonProperty("Name",name),
                         new JsonProperty("Unit", unit.ToString()),
                         new JsonProperty("Tags", tags.Tags)
                     }.Concat(properties))
            });
        }

        protected override void ReportGauge(string name, double value, Unit unit, MetricTags tags)
        {
            if (_isDisposed) return;
            if (!double.IsNaN(value) && !double.IsInfinity(value))
            {
                Pack("Gauge", name, unit, tags, new[] {
                    new JsonProperty("Value", value),
                });
            }
        }

        private Dictionary<String, Int64> CounterLastValue = new Dictionary<string, long>();
        private Dictionary<String, Int64> CounterPropertiesLastValue = new Dictionary<string, long>();

        protected override void ReportCounter(string name, CounterValue value, Unit unit, MetricTags tags)
        {
            if (_isDisposed) return;
            var itemProperties = value.Items.SelectMany(i => (new[]
            {
                new JsonProperty(SanitizeJsonPropertyName(i.Item)+ "-Count", i.Count),
                new JsonProperty(SanitizeJsonPropertyName(i.Item) + "-Percent", i.Percent),
            }));

            Pack("Counter", name, unit, tags, new[] {
                new JsonProperty("Count", value.Count),
            }.Concat(itemProperties));

            Int64 lastValue;
            if (!CounterLastValue.TryGetValue(name, out lastValue))
                lastValue = 0;

            CounterLastValue[name] = value.Count;

            var itemPropertiesDiff = value.Items.SelectMany(i =>
            {
                var pName = SanitizeJsonPropertyName(i.Item);
                var itemPropName = name + "-" + pName;
                Int64 pLastValue;
                if (!CounterPropertiesLastValue.TryGetValue(itemPropName, out pLastValue))
                    lastValue = 0;
                CounterPropertiesLastValue[itemPropName] = i.Count;

                var item = (new[]
                {
                    new JsonProperty(pName+ "-Count", i.Count - pLastValue)
                });
                return item;
            });

            Pack("CounterDiff", name, unit, tags, new[] {
                new JsonProperty("Count", value.Count - lastValue),
            }.Concat(itemPropertiesDiff));
        }

        private static string SanitizeJsonPropertyName(String name)
        {
            return name.Replace('.', '_');
        }

        private Dictionary<String, Int64> MeterLastValue = new Dictionary<string, long>();
        private Dictionary<String, Int64> MeterPropertiesLastValue = new Dictionary<string, long>();


        protected override void ReportMeter(string name, MeterValue value, Unit unit, TimeUnit rateUnit, MetricTags tags)
        {
            if (_isDisposed) return;
            var itemProperties = value.Items.SelectMany(i =>
            {
                var pName = SanitizeJsonPropertyName(i.Item);
                return new[]
                {
                    new JsonProperty(pName + "-Count", i.Value.Count),
                    new JsonProperty(pName + "-Percent", i.Percent),
                    new JsonProperty(pName + "-Mean Rate", i.Value.MeanRate),
                    new JsonProperty(pName + "-1 Min Rate", i.Value.OneMinuteRate),
                    new JsonProperty(pName + "-5 Min Rate", i.Value.FiveMinuteRate),
                    new JsonProperty(pName + "-15 Min Rate", i.Value.FifteenMinuteRate)
                };
            });

            Pack("Meter", name, unit, tags, new[] {
                new JsonProperty("Count", value.Count),
                new JsonProperty("Mean-Rate", value.MeanRate),
                new JsonProperty("1-Min-Rate", value.OneMinuteRate),
                new JsonProperty("5-Min-Rate", value.FiveMinuteRate),
                new JsonProperty("15-Min-Rate", value.FifteenMinuteRate)
            }.Concat(itemProperties));


            Int64 lastValue;
            if (!MeterLastValue.TryGetValue(name, out lastValue))
                lastValue = 0;
            MeterLastValue[name] = value.Count;


            var itemPropertiesDiff = value.Items.SelectMany(i =>
            {
                var pName = SanitizeJsonPropertyName(i.Item);
                var itemPropName = name + "-" + pName;
                Int64 pLastValue;
                if (!MeterPropertiesLastValue.TryGetValue(itemPropName, out pLastValue))
                    lastValue = 0;
                MeterPropertiesLastValue[itemPropName] = i.Value.Count;
                return new[]
                {
                    new JsonProperty(pName + "-Count", i.Value.Count - pLastValue)
                };
            });

            Pack("MeterDiff", name, unit, tags, new[] {
                new JsonProperty("Count", value.Count - lastValue)
            }.Concat(itemPropertiesDiff));
        }

        protected override void ReportHistogram(string name, HistogramValue value, Unit unit, MetricTags tags)
        {
            if (_isDisposed) return;
            Pack("Histogram", name, unit, tags, new[] {
                new JsonProperty("Total-Count",value.Count),
                new JsonProperty("Last", value.LastValue),
                new JsonProperty("Last-User-Value", value.LastUserValue),
                new JsonProperty("Min",value.Min),
                new JsonProperty("Min-User-Value",value.MinUserValue),
                new JsonProperty("Mean",value.Mean),
                new JsonProperty("Max",value.Max),
                new JsonProperty("Max-User-Value",value.MaxUserValue),
                new JsonProperty("StdDev",value.StdDev),
                new JsonProperty("Median",value.Median),
                new JsonProperty("Percentile-75",value.Percentile75),
                new JsonProperty("Percentile-95",value.Percentile95),
                new JsonProperty("Percentile-98",value.Percentile98),
                new JsonProperty("Percentile-99",value.Percentile99),
                new JsonProperty("Percentile-99_9" ,value.Percentile999),
                new JsonProperty("Sample-Size", value.SampleSize)
            });
        }

        protected override void ReportTimer(string name, TimerValue value, Unit unit, TimeUnit rateUnit, TimeUnit durationUnit, MetricTags tags)
        {
            if (_isDisposed) return;
            Pack("Timer", name, unit, tags, new[] {
                new JsonProperty("Total-Count",value.Rate.Count),
                new JsonProperty("Active-Sessions",value.ActiveSessions),
                new JsonProperty("Mean-Rate", value.Rate.MeanRate),
                new JsonProperty("1-Min-Rate", value.Rate.OneMinuteRate),
                new JsonProperty("5-Min-Rate", value.Rate.FiveMinuteRate),
                new JsonProperty("15-Min-Rate", value.Rate.FifteenMinuteRate),
                new JsonProperty("Last", value.Histogram.LastValue),
                new JsonProperty("Last-User-Value", value.Histogram.LastUserValue),
                new JsonProperty("Min",value.Histogram.Min),
                new JsonProperty("Min-User-Value",value.Histogram.MinUserValue),
                new JsonProperty("Mean",value.Histogram.Mean),
                new JsonProperty("Max",value.Histogram.Max),
                new JsonProperty("Max-User-Value",value.Histogram.MaxUserValue),
                new JsonProperty("StdDev",value.Histogram.StdDev),
                new JsonProperty("Median",value.Histogram.Median),
                new JsonProperty("Percentile-75",value.Histogram.Percentile75),
                new JsonProperty("Percentile-95",value.Histogram.Percentile95),
                new JsonProperty("Percentile-98",value.Histogram.Percentile98),
                new JsonProperty("Percentile-99",value.Histogram.Percentile99),
                new JsonProperty("Percentile-99_9" ,value.Histogram.Percentile999),
                new JsonProperty("Sample-Size", value.Histogram.SampleSize)
            });
        }

        protected override void ReportHealth(HealthStatus status)
        {
            if (_isDisposed) return;
        }

        protected override void StartContext(string contextName)
        {
            base.StartContext(contextName);
        }

        protected override void EndContext(string contextName)
        {
            base.EndContext(contextName);
        }

        protected override void StartMetricGroup(string metricName)
        {
            base.StartMetricGroup(metricName);
        }

        protected override void EndMetricGroup(string metricName)
        {
            base.EndMetricGroup(metricName);
        }

        private Boolean _isDisposed;
        protected void Dispose(Boolean isDisposing)
        {
            if (_isDisposed) return;
            _isDisposed = true;
            
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
