package anomaly

import (
	"fmt"
	"math"
	"math/cmplx"
	"sort"
	"sync"
	"time"
)

// TimeSeriesAnalysis represents the result of analyzing a time-series metric.
type TimeSeriesAnalysis struct {
	Metric      string           `json:"metric"`
	Source      string           `json:"source"`
	DataPoints  int              `json:"data_points"`
	TimeRange   TimeRange        `json:"time_range"`
	Trend       TrendAnalysis    `json:"trend"`
	Seasonality SeasonalityInfo  `json:"seasonality"`
	Outliers    []OutlierPoint   `json:"outliers"`
	Correlations []Correlation   `json:"correlations,omitempty"`
	Statistics  DescriptiveStats `json:"statistics"`
	ChartData   []ChartPoint     `json:"chart_data"`
	Timestamp   string           `json:"timestamp"`
}

// TimeRange represents the time span of analyzed data.
type TimeRange struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	Duration string `json:"duration"`
}

// TrendAnalysis contains Mann-Kendall trend test results.
type TrendAnalysis struct {
	Direction   string  `json:"direction"`   // "increasing", "decreasing", "no_trend"
	Strength    float64 `json:"strength"`    // 0.0-1.0
	Slope       float64 `json:"slope"`       // Sen's slope
	Significant bool    `json:"significant"`
	PValue      float64 `json:"p_value"`
}

// SeasonalityInfo contains FFT-based seasonality detection results.
type SeasonalityInfo struct {
	Detected    bool    `json:"detected"`
	Period      float64 `json:"period"`       // in data points
	PeriodLabel string  `json:"period_label"` // "hourly", "daily", "weekly"
	Amplitude   float64 `json:"amplitude"`
	Confidence  float64 `json:"confidence"` // 0.0-1.0
}

// OutlierPoint is a data point flagged as an outlier.
type OutlierPoint struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
	Expected  float64 `json:"expected"`
	ZScore    float64 `json:"z_score"`
	Type      string  `json:"type"` // "spike", "drop"
}

// Correlation represents a correlation between two metrics.
type Correlation struct {
	Source1   string  `json:"source_1"`
	Source2   string  `json:"source_2"`
	Pearson   float64 `json:"pearson"`
	Spearman  float64 `json:"spearman"`
	Strength  string  `json:"strength"`  // "strong", "moderate", "weak", "none"
	Direction string  `json:"direction"` // "positive", "negative"
}

// DescriptiveStats contains basic statistical measures.
type DescriptiveStats struct {
	Count  int     `json:"count"`
	Mean   float64 `json:"mean"`
	Median float64 `json:"median"`
	StdDev float64 `json:"std_dev"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	P95    float64 `json:"p95"`
	P99    float64 `json:"p99"`
}

// ChartPoint is a point formatted for chart rendering.
type ChartPoint struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
	Anomaly   bool    `json:"anomaly"`
}

// TimeSeriesAnalyzer provides advanced time-series analysis.
type TimeSeriesAnalyzer struct {
	mu      sync.RWMutex
	buffers map[string]*ringBuffer
}

// NewTimeSeriesAnalyzer creates a new analyzer sharing detector buffers.
func NewTimeSeriesAnalyzer() *TimeSeriesAnalyzer {
	return &TimeSeriesAnalyzer{
		buffers: make(map[string]*ringBuffer),
	}
}

// RecordPoint stores a data point for time-series analysis.
func (a *TimeSeriesAnalyzer) RecordPoint(metric MetricType, source string, value float64, ts time.Time) {
	key := fmt.Sprintf("%s:%s", metric, source)
	a.mu.Lock()
	buf, ok := a.buffers[key]
	if !ok {
		buf = newRingBuffer(8192)
		a.buffers[key] = buf
	}
	a.mu.Unlock()
	buf.Add(DataPoint{Timestamp: ts, Value: value, Metric: metric, Source: source})
}

// Analyze performs comprehensive time-series analysis on a metric.
func (a *TimeSeriesAnalyzer) Analyze(metric MetricType, source string, since time.Time) (*TimeSeriesAnalysis, error) {
	key := fmt.Sprintf("%s:%s", metric, source)
	a.mu.RLock()
	buf, ok := a.buffers[key]
	a.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no data found for metric %s source %s", metric, source)
	}

	points := buf.Snapshot(since)
	if len(points) < 3 {
		return nil, fmt.Errorf("insufficient data: need at least 3 points, have %d", len(points))
	}

	values := make([]float64, len(points))
	for i, p := range points {
		values[i] = p.Value
	}

	analysis := &TimeSeriesAnalysis{
		Metric:     string(metric),
		Source:     source,
		DataPoints: len(points),
		TimeRange: TimeRange{
			Start:    points[0].Timestamp.UTC().Format(time.RFC3339),
			End:      points[len(points)-1].Timestamp.UTC().Format(time.RFC3339),
			Duration: points[len(points)-1].Timestamp.Sub(points[0].Timestamp).String(),
		},
		Trend:       mannKendallTest(values),
		Seasonality: detectSeasonalityFFT(values),
		Outliers:    findOutliers(points, values),
		Statistics:  computeStats(values),
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
	}

	// Chart data
	mean, stddev := meanStddev(values)
	threshold := mean + 3*stddev
	for _, p := range points {
		analysis.ChartData = append(analysis.ChartData, ChartPoint{
			Timestamp: p.Timestamp.UTC().Format(time.RFC3339),
			Value:     p.Value,
			Anomaly:   p.Value > threshold || p.Value < mean-3*stddev,
		})
	}

	return analysis, nil
}

// CorrelateMetrics computes Pearson and Spearman correlations between two metrics.
func (a *TimeSeriesAnalyzer) CorrelateMetrics(metric1 MetricType, source1 string, metric2 MetricType, source2 string, since time.Time) (*Correlation, error) {
	key1 := fmt.Sprintf("%s:%s", metric1, source1)
	key2 := fmt.Sprintf("%s:%s", metric2, source2)

	a.mu.RLock()
	buf1, ok1 := a.buffers[key1]
	buf2, ok2 := a.buffers[key2]
	a.mu.RUnlock()

	if !ok1 || !ok2 {
		return nil, fmt.Errorf("data not found for one or both metrics")
	}

	points1 := buf1.Snapshot(since)
	points2 := buf2.Snapshot(since)

	n := len(points1)
	if len(points2) < n {
		n = len(points2)
	}
	if n < 3 {
		return nil, fmt.Errorf("insufficient overlapping data points")
	}

	vals1 := make([]float64, n)
	vals2 := make([]float64, n)
	for i := 0; i < n; i++ {
		vals1[i] = points1[i].Value
		vals2[i] = points2[i].Value
	}

	pearson := pearsonCorrelation(vals1, vals2)
	spearman := spearmanCorrelation(vals1, vals2)

	corr := &Correlation{
		Source1:  source1,
		Source2:  source2,
		Pearson:  pearson,
		Spearman: spearman,
	}

	absPearson := math.Abs(pearson)
	switch {
	case absPearson > 0.7:
		corr.Strength = "strong"
	case absPearson > 0.4:
		corr.Strength = "moderate"
	case absPearson > 0.2:
		corr.Strength = "weak"
	default:
		corr.Strength = "none"
	}
	if pearson >= 0 {
		corr.Direction = "positive"
	} else {
		corr.Direction = "negative"
	}

	return corr, nil
}

// mannKendallTest performs the Mann-Kendall trend test.
func mannKendallTest(values []float64) TrendAnalysis {
	n := len(values)
	if n < 4 {
		return TrendAnalysis{Direction: "no_trend", Significant: false}
	}

	s := 0.0
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			diff := values[j] - values[i]
			if diff > 0 {
				s++
			} else if diff < 0 {
				s--
			}
		}
	}

	nf := float64(n)
	variance := nf * (nf - 1) * (2*nf + 5) / 18.0

	var z float64
	if variance > 0 {
		sqrtVar := math.Sqrt(variance)
		if s > 0 {
			z = (s - 1) / sqrtVar
		} else if s < 0 {
			z = (s + 1) / sqrtVar
		}
	}

	// Sen's slope
	var slopes []float64
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			slopes = append(slopes, (values[j]-values[i])/float64(j-i))
		}
	}
	sort.Float64s(slopes)
	sensSlope := 0.0
	if len(slopes) > 0 {
		sensSlope = slopes[len(slopes)/2]
	}

	absZ := math.Abs(z)
	pValue := 2 * (1 - normalCDF(absZ))

	trend := TrendAnalysis{
		Slope:       sensSlope,
		PValue:      pValue,
		Significant: pValue < 0.05,
	}

	strength := math.Abs(s) / (nf * (nf - 1) / 2)
	trend.Strength = strength

	if !trend.Significant {
		trend.Direction = "no_trend"
	} else if s > 0 {
		trend.Direction = "increasing"
	} else {
		trend.Direction = "decreasing"
	}

	return trend
}

// detectSeasonalityFFT uses DFT to detect seasonal patterns.
func detectSeasonalityFFT(values []float64) SeasonalityInfo {
	n := len(values)
	if n < 8 {
		return SeasonalityInfo{Detected: false}
	}

	magnitudes := make([]float64, n/2)
	for k := 1; k < n/2; k++ {
		var sum complex128
		for t := 0; t < n; t++ {
			angle := -2 * math.Pi * float64(k) * float64(t) / float64(n)
			sum += complex(values[t], 0) * cmplx.Rect(1, angle)
		}
		magnitudes[k] = cmplx.Abs(sum) / float64(n)
	}

	maxMag := 0.0
	maxK := 1
	for k := 1; k < len(magnitudes); k++ {
		if magnitudes[k] > maxMag {
			maxMag = magnitudes[k]
			maxK = k
		}
	}

	var sumMag float64
	for _, m := range magnitudes[1:] {
		sumMag += m
	}
	meanMag := sumMag / float64(len(magnitudes)-1)

	period := float64(n) / float64(maxK)
	confidence := 0.0
	if meanMag > 0 {
		confidence = math.Min(maxMag/meanMag/5.0, 1.0)
	}

	info := SeasonalityInfo{
		Detected:   confidence > 0.3,
		Period:     period,
		Amplitude:  maxMag,
		Confidence: confidence,
	}

	switch {
	case period <= 2:
		info.PeriodLabel = "sub-hourly"
	case period <= 25:
		info.PeriodLabel = "hourly"
	case period <= 170:
		info.PeriodLabel = "daily"
	default:
		info.PeriodLabel = "weekly"
	}

	return info
}

func findOutliers(points []DataPoint, values []float64) []OutlierPoint {
	mean, stddev := meanStddev(values)
	if stddev == 0 {
		return nil
	}

	var outliers []OutlierPoint
	for i, v := range values {
		z := (v - mean) / stddev
		if math.Abs(z) > 3.0 {
			outlierType := "spike"
			if v < mean {
				outlierType = "drop"
			}
			outliers = append(outliers, OutlierPoint{
				Timestamp: points[i].Timestamp.UTC().Format(time.RFC3339),
				Value:     v,
				Expected:  mean,
				ZScore:    z,
				Type:      outlierType,
			})
		}
	}
	return outliers
}

func computeStats(values []float64) DescriptiveStats {
	if len(values) == 0 {
		return DescriptiveStats{}
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	mean, stddev := meanStddev(values)

	return DescriptiveStats{
		Count:  len(values),
		Mean:   mean,
		Median: sorted[len(sorted)/2],
		StdDev: stddev,
		Min:    sorted[0],
		Max:    sorted[len(sorted)-1],
		P95:    percentile(sorted, 95),
		P99:    percentile(sorted, 99),
	}
}

func pearsonCorrelation(x, y []float64) float64 {
	n := len(x)
	if n != len(y) || n < 2 {
		return 0
	}

	mx, _ := meanStddev(x)
	my, _ := meanStddev(y)

	var num, denomX, denomY float64
	for i := 0; i < n; i++ {
		dx := x[i] - mx
		dy := y[i] - my
		num += dx * dy
		denomX += dx * dx
		denomY += dy * dy
	}

	denom := math.Sqrt(denomX * denomY)
	if denom == 0 {
		return 0
	}
	return num / denom
}

func spearmanCorrelation(x, y []float64) float64 {
	n := len(x)
	if n < 2 {
		return 0
	}
	rx := ranks(x)
	ry := ranks(y)
	return pearsonCorrelation(rx, ry)
}

func ranks(values []float64) []float64 {
	type indexedValue struct {
		value float64
		index int
	}
	iv := make([]indexedValue, len(values))
	for i, v := range values {
		iv[i] = indexedValue{v, i}
	}
	sort.Slice(iv, func(i, j int) bool {
		return iv[i].value < iv[j].value
	})
	result := make([]float64, len(values))
	for rank, item := range iv {
		result[item.index] = float64(rank + 1)
	}
	return result
}

func normalCDF(x float64) float64 {
	return 0.5 * math.Erfc(-x/math.Sqrt2)
}
