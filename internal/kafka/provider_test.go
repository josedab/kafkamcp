package kafka

import "testing"

func TestNativeOperationProviderInfo(t *testing.T) {
	manager := &MockKafkaManager{}
	provider := NewNativeOperationProvider(manager)
	if provider.ProviderInfo().Kind != ProviderNative {
		t.Fatalf("expected native provider, got %+v", provider.ProviderInfo())
	}
	if provider.ProviderInfo().Name == "" {
		t.Fatal("provider name must be populated")
	}
	if _, ok := provider.(*nativeOperationProvider); !ok {
		t.Fatalf("expected native provider wrapper, got %T", provider)
	}
}
