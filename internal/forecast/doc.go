// Package forecast provides ML-based capacity forecasting for Kafka topics.
//
// It uses linear regression and seasonal decomposition to predict throughput
// growth, storage needs, and consumer lag trends over a configurable horizon.
package forecast
