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

        [Test]
        public void Smoke_test_for_meters()
        {
            Metric.Config
                .WithReporting(r => (reports = r).WithElasticSearchReport(
                        TimeSpan.FromSeconds(1), sutConfigurer
                    ));
            Meter meter = Metric.Meter("test1", Unit.Calls, TimeUnit.Milliseconds);

            meter.Mark();
            Thread.Sleep(3000);
            var records = base.GetAllMeter("es-test", "test1");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 1));

            records = base.GetAllMeterDiff("es-test", "test1");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 1));

            meter.Mark();
            Thread.Sleep(2000);
            records = base.GetAllMeter("es-test", "test1");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 2));

            records = base.GetAllMeterDiff("es-test", "test1");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 1));
        }

        [Test]
        public void Smoke_test_for_meters_with_context()
        {
            Metric.Config
                .WithReporting(r => (reports = r).WithElasticSearchReport(
                        TimeSpan.FromSeconds(1), sutConfigurer
                    ));
            Meter meter = Metric.Meter("test2", Unit.Calls, TimeUnit.Milliseconds);

            meter.Mark("item1");
            meter.Mark("item2", 2);
            Thread.Sleep(3000);

            var records = base.GetAllMeter("es-test", "test2");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 3));
            Assert.That(records.Any(r => r.Value<Int32>("item1-Count") == 1));
            Assert.That(records.Any(r => r.Value<Int32>("item2-Count") == 2));

            //Delete index
            DeleteIndexesByPrefix(prefix);

            meter.Mark("item1", 3);
            meter.Mark("item3", 2);
            Thread.Sleep(3000);

            records = base.GetAllMeter("es-test", "test2");
            var record = records.First(r => r.Value<Int32>("Count") == 8);
           
            Assert.That(record.Value<Int32>("item1-Count"), Is.EqualTo(4));
            Assert.That(record.Value<Int32>("item3-Count"), Is.EqualTo(2));
            Assert.That(record.Value<Int32>("item2-Count"), Is.EqualTo(2));

            records = base.GetAllMeterDiff("es-test", "test2");
            record = records.Single(r => r.Value<Int32>("Count") == 5);

            Assert.That(record.Value<Int32>("item1-Count"), Is.EqualTo(3));
            Assert.That(record.Value<Int32>("item3-Count"), Is.EqualTo(2));
            Assert.That(record.Value<Int32>("item2-Count"), Is.EqualTo(0));
        }

    }
}
