using System;
using Metrics.ElasticSearch;
using Metrics.Reports;
using System.Configuration;

namespace Metrics.Net.Es
{
    public static class MetricsElasticSearchConfigExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reports"></param>
        /// <param name="reportToConfigure">The report object that needs to be configured</param>
        /// <param name="interval"></param>
        /// <returns></returns>
        public static MetricsReports WithElasticSearchReport(
            this MetricsReports reports,
            TimeSpan interval,
            Action<ElasticSearchReport> reportToConfigure)
        {
            ElasticSearchReport report = new ElasticSearchReport();
            reportToConfigure(report);
            String validation = report.Validate();
            if (!String.IsNullOrEmpty(validation))
                throw new ConfigurationErrorsException("Error in Elastic Search Metrics Configuration: " + validation);

            report.Init();
            return reports.WithReport(report, interval);
        }


    }
}
