package kafka

import "testing"

func TestValidateTopicConfigs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		configs map[string]string
		wantErr bool
	}{
		{"nil configs", nil, false},
		{"empty configs", map[string]string{}, false},
		{"safe configs", map[string]string{"retention.ms": "86400000", "cleanup.policy": "compact"}, false},
		{"dangerous min.insync.replicas", map[string]string{"min.insync.replicas": "1"}, true},
		{"dangerous unclean.leader", map[string]string{"unclean.leader.election.enable": "true"}, true},
		{"dangerous message.downconversion", map[string]string{"message.downconversion.enable": "false"}, true},
		{"case insensitive", map[string]string{"Min.Insync.Replicas": "2"}, true},
		{"mixed safe and dangerous", map[string]string{"retention.ms": "1000", "min.insync.replicas": "1"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTopicConfigs(tt.configs)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTopicConfigs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedactSensitiveConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  map[string]string
		want map[string]string
	}{
		{
			"no sensitive keys",
			map[string]string{"connector.class": "FileSource", "topic": "test"},
			map[string]string{"connector.class": "FileSource", "topic": "test"},
		},
		{
			"password redacted",
			map[string]string{"db.password": "secret123", "topic": "test"},
			map[string]string{"db.password": "**REDACTED**", "topic": "test"},
		},
		{
			"multiple sensitive keys",
			map[string]string{"api.key": "k", "ssl.keystore.password": "p", "name": "conn1"},
			map[string]string{"api.key": "**REDACTED**", "ssl.keystore.password": "**REDACTED**", "name": "conn1"},
		},
		{
			"empty sensitive value not redacted",
			map[string]string{"db.password": "", "topic": "test"},
			map[string]string{"db.password": "", "topic": "test"},
		},
		{
			"case insensitive match",
			map[string]string{"DB.Password": "secret"},
			map[string]string{"DB.Password": "**REDACTED**"},
		},
		{
			"token in key",
			map[string]string{"auth.token": "abc123"},
			map[string]string{"auth.token": "**REDACTED**"},
		},
		{
			"nil config",
			nil,
			map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactSensitiveConfig(tt.cfg)
			if len(got) != len(tt.want) {
				t.Errorf("length mismatch: got %d, want %d", len(got), len(tt.want))
				return
			}
			for k, wantV := range tt.want {
				if gotV, ok := got[k]; !ok || gotV != wantV {
					t.Errorf("key %q: got %q, want %q", k, gotV, wantV)
				}
			}
		})
	}
}
