using Metrics.Net.Es;
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
        }


        [Test]
        public void Smoke_test_for_counter()
        {
            Metric.Config
                .WithReporting(r => r.WithElasticSearchReport(
                        TimeSpan.FromSeconds(1), sutConfigurer
                    ));
            Counter c = Metric.Counter("test", Unit.Calls);
            c.Increment(3);
            Thread.Sleep(2000);
            c.Increment(5);
            Thread.Sleep(2000);

            var records = base.GetAllCounters("es-test");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 3));
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 8));

            records = base.GetAllCountersDiff("es-test");
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 3));
            Assert.That(records.Any(r => r.Value<Int32>("Count") == 5));
        }
    }
}
