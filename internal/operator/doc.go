// Package operator provides a Kubernetes operator for managing KafkaMCP
// instances as custom resources. It handles CRD generation, deployment
// manifests, instance lifecycle (create, update, delete, scale), health
// checks, auto-scaling, and a reconciliation control loop.
package operator
