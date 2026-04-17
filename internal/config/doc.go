// Package config handles loading, validating, and applying defaults for
// KafkaMCP's YAML configuration file.
//
// It supports environment variable expansion (${VAR} and ${VAR:-default}),
// warns on world-readable config files that may contain credentials, and
// validates cluster, transport, and SASL settings at startup.
package config
