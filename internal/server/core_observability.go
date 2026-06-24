package server

import (
	dto "github.com/prometheus/client_model/go"
)

func aggregateMetricFamily(family *dto.MetricFamily) map[string]any {
	result := map[string]any{
		"type":    family.GetType().String(),
		"series":  len(family.Metric),
		"samples": float64(0),
	}
	var total float64
	for _, metric := range family.Metric {
		switch family.GetType() {
		case dto.MetricType_COUNTER:
			total += metric.GetCounter().GetValue()
		case dto.MetricType_GAUGE:
			total += metric.GetGauge().GetValue()
		case dto.MetricType_HISTOGRAM:
			total += float64(metric.GetHistogram().GetSampleCount())
		case dto.MetricType_SUMMARY:
			total += float64(metric.GetSummary().GetSampleCount())
		case dto.MetricType_UNTYPED:
			total += metric.GetUntyped().GetValue()
		}
	}
	result["samples"] = total
	return result
}
