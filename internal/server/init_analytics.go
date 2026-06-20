package server

import (
	"log/slog"

	"github.com/josedab/kafkamcp/internal/lagdiag"
)

// Analytics groups AI-powered analysis and intelligence subsystems.
type Analytics struct {
	lagAnalyzer *lagdiag.Analyzer // consumer lag root cause analysis
}

// newAnalytics initializes all analytics subsystems.
func newAnalytics(_ *slog.Logger) Analytics {
	return Analytics{
		lagAnalyzer: lagdiag.NewAnalyzer(),
	}
}
