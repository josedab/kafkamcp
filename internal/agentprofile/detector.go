// Package agentprofile provides ML-based behavioral threat detection for agents,
// profiling access patterns and detecting anomalous behavior such as data exfiltration
// attempts, privilege escalation, and unusual bulk reads.
package agentprofile

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// ThreatType categorizes detected threats.
type ThreatType string

// ThreatDataExfiltration and related values define constants for this package.
const (
	ThreatDataExfiltration    ThreatType = "data_exfiltration"
	ThreatPrivilegeEscalation ThreatType = "privilege_escalation"
	ThreatBulkRead            ThreatType = "unusual_bulk_read"
	ThreatOffHoursAccess      ThreatType = "off_hours_access"
	ThreatNewTopicAccess      ThreatType = "new_topic_access"
	ThreatRatioAnomaly        ThreatType = "read_write_ratio_anomaly"
)

// Severity indicates how serious a threat alert is.
type Severity string

// SeverityLow and related values define constants for this package.
const (
	SeverityLow      Severity = "low"
	SeverityMedium   Severity = "medium"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

// ThreatAlert represents a detected anomalous behavior.
type ThreatAlert struct {
	ID          string         `json:"id"`
	AgentID     string         `json:"agent_id"`
	ThreatType  ThreatType     `json:"threat_type"`
	Severity    Severity       `json:"severity"`
	Score       float64        `json:"score"`
	Description string         `json:"description"`
	Details     map[string]any `json:"details,omitempty"`
	DetectedAt  time.Time      `json:"detected_at"`
}

// AgentProfile stores the baseline behavior for an agent.
type AgentProfile struct {
	AgentID          string           `json:"agent_id"`
	TopicsAccessed   map[string]int64 `json:"topics_accessed"`
	ReadCount        int64            `json:"read_count"`
	WriteCount       int64            `json:"write_count"`
	HourlyActivity   [24]int64        `json:"hourly_activity"`
	DailyReadCounts  []float64        `json:"-"`
	DailyWriteCounts []float64        `json:"-"`
	FirstSeen        time.Time        `json:"first_seen"`
	LastSeen         time.Time        `json:"last_seen"`
	BaselineReady    bool             `json:"baseline_ready"`
}

// AccessEvent records a single agent access.
type AccessEvent struct {
	AgentID   string    `json:"agent_id"`
	Tool      string    `json:"tool"`
	Topic     string    `json:"topic,omitempty"`
	Action    string    `json:"action"` // "read" or "write"
	Count     int       `json:"count"`
	Timestamp time.Time `json:"timestamp"`
}

// DetectorConfig configures the threat detection engine.
type DetectorConfig struct {
	BaselineDays        int     `json:"baseline_days" yaml:"baseline_days"`
	ZScoreThreshold     float64 `json:"z_score_threshold" yaml:"z_score_threshold"`
	IQRMultiplier       float64 `json:"iqr_multiplier" yaml:"iqr_multiplier"`
	MaxAlerts           int     `json:"max_alerts" yaml:"max_alerts"`
	BulkReadThreshold   int     `json:"bulk_read_threshold" yaml:"bulk_read_threshold"`
	ExfilBytesThreshold int64   `json:"exfil_bytes_threshold" yaml:"exfil_bytes_threshold"`
}

// DefaultDetectorConfig returns sensible defaults.
func DefaultDetectorConfig() DetectorConfig {
	return DetectorConfig{
		BaselineDays:        7,
		ZScoreThreshold:     3.0,
		IQRMultiplier:       1.5,
		MaxAlerts:           1000,
		BulkReadThreshold:   1000,
		ExfilBytesThreshold: 100 * 1024 * 1024, // 100MB
	}
}

// Detector detects anomalous agent behavior using statistical models.
type Detector struct {
	mu       sync.RWMutex
	config   DetectorConfig
	profiles map[string]*AgentProfile
	alerts   []ThreatAlert
	logger   *slog.Logger
}

// NewDetector creates a new threat detector.
func NewDetector(cfg DetectorConfig, logger *slog.Logger) *Detector {
	return &Detector{
		config:   cfg,
		profiles: make(map[string]*AgentProfile),
		alerts:   make([]ThreatAlert, 0),
		logger:   logger,
	}
}

// RecordAccess records an agent access event and checks for anomalies.
func (d *Detector) RecordAccess(event AccessEvent) []ThreatAlert {
	d.mu.Lock()
	defer d.mu.Unlock()

	profile := d.getOrCreateProfile(event.AgentID)
	now := event.Timestamp
	if now.IsZero() {
		now = time.Now()
	}

	// Update profile
	if event.Topic != "" {
		profile.TopicsAccessed[event.Topic]++
	}
	if event.Action == "read" {
		profile.ReadCount += int64(event.Count)
		profile.DailyReadCounts = appendWindow(profile.DailyReadCounts, float64(event.Count), 30)
	} else if event.Action == "write" {
		profile.WriteCount += int64(event.Count)
		profile.DailyWriteCounts = appendWindow(profile.DailyWriteCounts, float64(event.Count), 30)
	}
	profile.HourlyActivity[now.Hour()]++
	profile.LastSeen = now

	if !profile.BaselineReady {
		if time.Since(profile.FirstSeen) >= time.Duration(d.config.BaselineDays)*24*time.Hour {
			profile.BaselineReady = true
		}
		return nil
	}

	// Run detection checks
	var alerts []ThreatAlert
	if alert := d.checkBulkRead(profile, event); alert != nil {
		alerts = append(alerts, *alert)
	}
	if alert := d.checkOffHoursAccess(profile, event, now); alert != nil {
		alerts = append(alerts, *alert)
	}
	if alert := d.checkNewTopicAccess(profile, event); alert != nil {
		alerts = append(alerts, *alert)
	}
	if alert := d.checkReadWriteRatio(profile); alert != nil {
		alerts = append(alerts, *alert)
	}

	for i := range alerts {
		d.addAlert(alerts[i])
	}
	return alerts
}

// GetProfile returns the behavioral profile for an agent.
func (d *Detector) GetProfile(agentID string) (*AgentProfile, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	p, ok := d.profiles[agentID]
	return p, ok
}

// GetAlerts returns recent threat alerts, optionally filtered by agent.
func (d *Detector) GetAlerts(agentID string, limit int) []ThreatAlert {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []ThreatAlert
	for i := len(d.alerts) - 1; i >= 0 && len(result) < limit; i-- {
		if agentID == "" || d.alerts[i].AgentID == agentID {
			result = append(result, d.alerts[i])
		}
	}
	return result
}

// Stats returns summary statistics about the detector.
func (d *Detector) Stats() map[string]any {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return map[string]any{
		"profiles_count":    len(d.profiles),
		"total_alerts":      len(d.alerts),
		"z_score_threshold": d.config.ZScoreThreshold,
		"baseline_days":     d.config.BaselineDays,
	}
}

func (d *Detector) getOrCreateProfile(agentID string) *AgentProfile {
	if p, ok := d.profiles[agentID]; ok {
		return p
	}
	p := &AgentProfile{
		AgentID:        agentID,
		TopicsAccessed: make(map[string]int64),
		FirstSeen:      time.Now(),
		LastSeen:       time.Now(),
	}
	d.profiles[agentID] = p
	return p
}

func (d *Detector) addAlert(alert ThreatAlert) {
	if len(d.alerts) >= d.config.MaxAlerts {
		d.alerts = d.alerts[1:]
	}
	d.alerts = append(d.alerts, alert)
	if d.logger != nil {
		d.logger.Warn("threat detected",
			"agent_id", alert.AgentID,
			"type", alert.ThreatType,
			"severity", alert.Severity,
			"score", alert.Score,
		)
	}
}

func (d *Detector) checkBulkRead(profile *AgentProfile, event AccessEvent) *ThreatAlert {
	if event.Action != "read" || event.Count < d.config.BulkReadThreshold {
		return nil
	}

	zScore := d.zScore(profile.DailyReadCounts, float64(event.Count))
	if zScore < d.config.ZScoreThreshold {
		return nil
	}

	severity := SeverityMedium
	if zScore > d.config.ZScoreThreshold*2 {
		severity = SeverityCritical
	} else if zScore > d.config.ZScoreThreshold*1.5 {
		severity = SeverityHigh
	}

	return &ThreatAlert{
		ID:          fmt.Sprintf("threat-%s-%s", profile.AgentID, randid.New("")),
		AgentID:     profile.AgentID,
		ThreatType:  ThreatBulkRead,
		Severity:    severity,
		Score:       zScore,
		Description: fmt.Sprintf("Agent %s performed bulk read of %d messages (z-score: %.2f)", profile.AgentID, event.Count, zScore),
		Details:     map[string]any{"count": event.Count, "z_score": zScore, "topic": event.Topic},
		DetectedAt:  time.Now(),
	}
}

func (d *Detector) checkOffHoursAccess(profile *AgentProfile, event AccessEvent, now time.Time) *ThreatAlert {
	hour := now.Hour()
	totalActivity := int64(0)
	for _, c := range profile.HourlyActivity {
		totalActivity += c
	}
	if totalActivity == 0 {
		return nil
	}

	hourFraction := float64(profile.HourlyActivity[hour]) / float64(totalActivity)
	if hourFraction > 0.01 {
		return nil // This hour has >1% of activity, not unusual
	}

	return &ThreatAlert{
		ID:          fmt.Sprintf("threat-%s-%s", profile.AgentID, randid.New("")),
		AgentID:     profile.AgentID,
		ThreatType:  ThreatOffHoursAccess,
		Severity:    SeverityLow,
		Score:       1.0 - hourFraction,
		Description: fmt.Sprintf("Agent %s accessed at unusual hour %d:00 (%.1f%% of baseline activity)", profile.AgentID, hour, hourFraction*100),
		Details:     map[string]any{"hour": hour, "hour_fraction": hourFraction},
		DetectedAt:  time.Now(),
	}
}

func (d *Detector) checkNewTopicAccess(profile *AgentProfile, event AccessEvent) *ThreatAlert {
	if event.Topic == "" {
		return nil
	}
	// Only alert if agent has a mature profile and accesses a topic for the first time
	if profile.TopicsAccessed[event.Topic] > 1 || len(profile.TopicsAccessed) < 3 {
		return nil
	}
	return &ThreatAlert{
		ID:          fmt.Sprintf("threat-%s-%s", profile.AgentID, randid.New("")),
		AgentID:     profile.AgentID,
		ThreatType:  ThreatNewTopicAccess,
		Severity:    SeverityLow,
		Score:       0.5,
		Description: fmt.Sprintf("Agent %s accessed new topic %q for the first time", profile.AgentID, event.Topic),
		Details:     map[string]any{"topic": event.Topic, "total_topics": len(profile.TopicsAccessed)},
		DetectedAt:  time.Now(),
	}
}

func (d *Detector) checkReadWriteRatio(profile *AgentProfile) *ThreatAlert {
	if profile.ReadCount+profile.WriteCount < 100 {
		return nil
	}
	ratio := float64(profile.ReadCount) / math.Max(float64(profile.WriteCount), 1.0)
	if len(profile.DailyReadCounts) < 7 {
		return nil
	}

	// Calculate historical ratio average
	totalReads := sum(profile.DailyReadCounts)
	totalWrites := sum(profile.DailyWriteCounts)
	historicalRatio := totalReads / math.Max(totalWrites, 1.0)

	if historicalRatio == 0 {
		return nil
	}

	deviation := math.Abs(ratio-historicalRatio) / historicalRatio
	if deviation < 2.0 {
		return nil
	}

	return &ThreatAlert{
		ID:          fmt.Sprintf("threat-%s-%s", profile.AgentID, randid.New("")),
		AgentID:     profile.AgentID,
		ThreatType:  ThreatRatioAnomaly,
		Severity:    SeverityMedium,
		Score:       deviation,
		Description: fmt.Sprintf("Agent %s read/write ratio %.1f deviates from baseline %.1f", profile.AgentID, ratio, historicalRatio),
		Details:     map[string]any{"current_ratio": ratio, "historical_ratio": historicalRatio, "deviation": deviation},
		DetectedAt:  time.Now(),
	}
}

// zScore calculates the z-score of a value relative to a historical window.
func (d *Detector) zScore(window []float64, value float64) float64 {
	if len(window) < 3 {
		return 0
	}
	mean := sum(window) / float64(len(window))
	variance := 0.0
	for _, v := range window {
		variance += (v - mean) * (v - mean)
	}
	stdDev := math.Sqrt(variance / float64(len(window)))
	if stdDev == 0 {
		return 0
	}
	return (value - mean) / stdDev
}

func sum(vals []float64) float64 {
	s := 0.0
	for _, v := range vals {
		s += v
	}
	return s
}

func appendWindow(window []float64, value float64, maxSize int) []float64 {
	window = append(window, value)
	if len(window) > maxSize {
		window = window[1:]
	}
	return window
}
