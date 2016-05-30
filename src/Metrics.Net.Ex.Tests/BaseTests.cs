using Metrics.Net.Es;
using Metrics.Reporters;
using Metrics.Reports;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Metrics.Net.Ex.Tests
{
    [TestFixture]
    public class BaseTests : BaseTestClass
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
                .WithIndexPrefix(prefix);
        }

        [TearDown]
        public void TearDown()
        {
            reports.StopAndClearAllReports();
            reports.Dispose();
        }


        [Test]
        public void Smoke_test_for_counter()
        {

            Metric.Config
                .WithReporting(r => (reports = r).WithElasticSearchReport(
                        TimeSpan.FromSeconds(1), sutConfigurer
                    ));
            Counter c = Metric.Counter("test", Unit.Calls);
            c.Increment(3);
            Thread.Sleep(2000);
            c.Increment(5);
            Thread.Sleep(2000);

            var records = base.GetAllCounters("es-test","test");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 3));
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 8));

            records = base.GetAllCountersDiff("es-test", "test");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 3));
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 5));
        }

        [Test]
        public void Smoke_test_for_counter_with_context()
        {

            Metric.Config
                .WithReporting(r => (reports = r).WithElasticSearchReport(
                        TimeSpan.FromSeconds(1), sutConfigurer
                    ));
            Counter c = Metric.Counter("test1", Unit.Calls);
            c.Increment("item1", 3);
            c.Increment("item2", 4);
            Thread.Sleep(2000);
            c.Increment("item1", 5);
            c.Increment("item3", 7);
            Thread.Sleep(2000);

            var records = base.GetAllCounters("es-test", "test1");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 7));
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 19));

            records = base.GetAllCountersDiff("es-test", "test1");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 7));
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 12));

            var lastRecord = records.Single(r => r.Value<Int32>("Count") == 12);
            Assert.That(lastRecord.Value<Int32>("item1-Count"), Is.EqualTo(5));
            Assert.That(lastRecord.Value<Int32>("item3-Count"), Is.EqualTo(7));
        }
    }
}
