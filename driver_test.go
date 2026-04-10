package driver

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// NewDriver — construction defaults
// ---------------------------------------------------------------------------

func TestNew_DefaultsConnTimeouts(t *testing.T) {
	d := NewDriver(Config{
		Addr:      ":3306",
		DBName:    "db",
		SourceDSN: "user:pass@tcp(127.0.0.1:3306)/db",
	}, &stubCoreAPI{})

	if d.cfg.ConnReadTimeout == 0 {
		t.Error("ConnReadTimeout was not defaulted — got zero duration")
	}
	if d.cfg.ConnWriteTimeout == 0 {
		t.Error("ConnWriteTimeout was not defaulted — got zero duration")
	}
}

func TestNew_ExplicitTimeouts_NotOverridden(t *testing.T) {
	const want = 5 * time.Second
	d := NewDriver(Config{
		Addr:             ":3306",
		DBName:           "db",
		SourceDSN:        "user:pass@tcp(127.0.0.1:3306)/db",
		ConnReadTimeout:  want,
		ConnWriteTimeout: want,
	}, &stubCoreAPI{})

	if d.cfg.ConnReadTimeout != want {
		t.Errorf("ConnReadTimeout: got %v, want %v", d.cfg.ConnReadTimeout, want)
	}
	if d.cfg.ConnWriteTimeout != want {
		t.Errorf("ConnWriteTimeout: got %v, want %v", d.cfg.ConnWriteTimeout, want)
	}
}

// TestNewDriver_GMSIsNonNil verifies that the GMS engine is fully wired during
// NewDriver — it should be non-nil and ready to serve queries before Run is
// called.
func TestNewDriver_GMSIsNonNil(t *testing.T) {
	d := NewDriver(Config{
		Addr:      ":3306",
		DBName:    "db",
		SourceDSN: "user:pass@tcp(127.0.0.1:3306)/db",
	}, &stubCoreAPI{})

	if d.gms == nil {
		t.Error("gms should be non-nil after NewDriver is called")
	}
}

func TestNew_SrvNilUntilRun(t *testing.T) {
	d := NewDriver(Config{
		Addr:      ":3306",
		DBName:    "db",
		SourceDSN: "user:pass@tcp(127.0.0.1:3306)/db",
	}, &stubCoreAPI{})

	if d.srv != nil {
		t.Error("srv should be nil before Run is called")
	}
}

// ---------------------------------------------------------------------------
// Stop — safe before Run
// ---------------------------------------------------------------------------

func TestStop_BeforeRun_ReturnsNil(t *testing.T) {
	d := NewDriver(Config{
		Addr:      ":0",
		DBName:    "testdb",
		SourceDSN: "user:pass@tcp(127.0.0.1:3306)/testdb",
	}, &stubCoreAPI{})

	if err := d.Stop(); err != nil {
		t.Fatalf("Stop() before Run returned a non-nil error: %v", err)
	}
}

func TestStop_BeforeRun_DoesNotPanic(t *testing.T) {
	d := NewDriver(Config{
		Addr:      ":0",
		DBName:    "testdb",
		SourceDSN: "user:pass@tcp(127.0.0.1:3306)/testdb",
	}, &stubCoreAPI{})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Stop() before Run panicked: %v", r)
		}
	}()

	_ = d.Stop()
}
