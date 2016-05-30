
using Metrics.Net.Es;
using Metrics.Reports;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Metrics.Net.Ex.Tests
{
    [TestFixture]
    public class IndexTests : BaseTestClass
    {
        private string prefix = "metrics-tests";
        private Action<ElasticSearchReport> sutConfigurer;
        private MetricsReports reports;

        /// <summary>
        /// 
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            esAddress = ConfigurationManager.AppSettings["es-server"];
            esPort = Int32.Parse(ConfigurationManager.AppSettings["es-port"]);
        }

        [SetUp]
        public void SetUp()
        {
            DeleteIndexesByPrefix(prefix);
            sutConfigurer = r => r
                .HostName(base.esAddress)
                .HostPort(base.esPort)
                .AddAlias("es-test")
                .SetIndexSuffixDateTimeFormat("yyyyMMdd")
                .WithIndexPrefix(prefix);
        }

        [Test]
        public void Verify_index_type()
        {
            Metric.Config
               .WithReporting(r => (reports = r).WithElasticSearchReport(
                       TimeSpan.FromSeconds(1), sutConfigurer
                   ));
            Counter c = Metric.Counter("test", Unit.Calls);
            c.Increment(3);
            Thread.Sleep(3000);

            var indexName = prefix + "-" + DateTime.UtcNow.ToString("yyyyMMdd");
            using (var client = new WebClient())
            {
                var result = client.DownloadString("http://" + esAddress + ":" + esPort + "/" + indexName + "/_search");
            }
        }
    }
}
