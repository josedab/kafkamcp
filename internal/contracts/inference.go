package contracts

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// InferredContract is a contract inferred from message samples.
type InferredContract struct {
	Topic      string        `json:"topic"`
	Name       string        `json:"name"`
	Version    string        `json:"version"`
	Quality    []QualityRule `json:"quality"`
	Confidence float64       `json:"confidence"`
	SampleSize int           `json:"sample_size"`
	Timestamp  string        `json:"timestamp"`
}

// InferContract generates a contract from message samples by analyzing field patterns.
func (e *Engine) InferContract(topic string, samples []map[string]any) *InferredContract {
	if len(samples) == 0 {
		return nil
	}

	fieldPresence := make(map[string]int)
	fieldValues := make(map[string][]string)

	for _, sample := range samples {
		for field, val := range sample {
			fieldPresence[field]++
			if str, ok := val.(string); ok {
				fieldValues[field] = append(fieldValues[field], str)
			}
		}
	}

	var rules []QualityRule
	sampleCount := len(samples)

	for field, count := range fieldPresence {
		if count == sampleCount {
			rules = append(rules, QualityRule{
				Field:    field,
				Rule:     "not_null",
				Severity: "error",
			})
		}

		if vals, ok := fieldValues[field]; ok && len(vals) > 0 {
			distinct := make(map[string]bool)
			for _, v := range vals {
				distinct[v] = true
			}
			if len(distinct) <= 10 && len(distinct) < len(vals)/2 {
				enumVals := make([]string, 0, len(distinct))
				for v := range distinct {
					enumVals = append(enumVals, v)
				}
				sort.Strings(enumVals)
				rules = append(rules, QualityRule{
					Field:    field,
					Rule:     "enum",
					Value:    strings.Join(enumVals, ","),
					Severity: "warning",
				})
			}
		}
	}

	confidence := 0.5
	if sampleCount >= 100 {
		confidence = 0.9
	} else if sampleCount >= 10 {
		confidence = 0.7
	}

	return &InferredContract{
		Topic:      topic,
		Name:       fmt.Sprintf("%s-contract", topic),
		Version:    "1.0.0-inferred",
		Quality:    rules,
		Confidence: confidence,
		SampleSize: sampleCount,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
	}
}
