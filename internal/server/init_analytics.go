package server

import (
	"log/slog"

	"github.com/kafkamcp/kafkamcp/internal/advisor"
	"github.com/kafkamcp/kafkamcp/internal/anomaly"
	"github.com/kafkamcp/kafkamcp/internal/cost"
	"github.com/kafkamcp/kafkamcp/internal/dataquality"
	"github.com/kafkamcp/kafkamcp/internal/evolution"
	"github.com/kafkamcp/kafkamcp/internal/forecast"
	"github.com/kafkamcp/kafkamcp/internal/graph"
	"github.com/kafkamcp/kafkamcp/internal/incident"
	"github.com/kafkamcp/kafkamcp/internal/lagdiag"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
	"github.com/kafkamcp/kafkamcp/internal/telemetry"
)

// Analytics groups AI-powered analysis and intelligence subsystems.
type Analytics struct {
	anomalyDetector    *anomaly.Detector           // AI-powered anomaly detection
	nlQueryEngine      *nlquery.Engine             // natural language query engine
	costAnalyzer       *cost.Analyzer              // Kafka cost analysis
	forecaster         *forecast.Forecaster        // intelligent capacity forecasting
	knowledgeGraph     *graph.KnowledgeGraph       // embedded knowledge graph
	lagAnalyzer        *lagdiag.Analyzer           // consumer lag root cause analysis
	schemaWorkflow     *evolution.WorkflowEngine   // schema evolution workflow engine
	timeseriesAnalyzer *anomaly.TimeSeriesAnalyzer // time-series analytics
	advisorEngine      *advisor.Engine             // intelligent topic advisor
	dataQualityEngine  *dataquality.Engine         // data quality scoring & SLA
	incidentCorrelator *incident.Correlator        // cross-cluster incident correlation
	telemetryDataLake  *telemetry.DataLake         // agent interaction telemetry data lake
}

// newAnalytics initializes all analytics subsystems.
func newAnalytics(logger *slog.Logger) Analytics {
	return Analytics{
		anomalyDetector:    anomaly.NewDetector(anomaly.DefaultConfig(), logger),
		nlQueryEngine:      nlquery.NewEngine(logger),
		costAnalyzer:       cost.NewAnalyzer(logger),
		forecaster:         forecast.NewForecaster(30),
		knowledgeGraph:     graph.NewKnowledgeGraph(logger),
		lagAnalyzer:        lagdiag.NewAnalyzer(),
		schemaWorkflow:     evolution.NewWorkflowEngine(),
		timeseriesAnalyzer: anomaly.NewTimeSeriesAnalyzer(),
		advisorEngine:      advisor.NewEngine(logger),
		dataQualityEngine:  dataquality.NewEngine(logger),
		incidentCorrelator: incident.NewCorrelator(incident.DefaultCorrelatorConfig(), logger),
		telemetryDataLake:  telemetry.NewDataLake(100000),
	}
}
