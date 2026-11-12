package mqtt

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The persisted format: every broker-owned stream is created stamped with the format
// this build writes, and a stream carrying anything else is refused — before its
// shape is judged, with guidance that does not lead with deleting a store some other
// broker can still read. See docs/ARCHITECTURE.md, "The persisted format".

// testFormatInfo is a stream as the format check sees it: name and a format stamp
// (absent when format is nil), beside a valid epoch so nothing else is the defect.
func testFormatInfo(name string, format *string) *StreamInfo {
	meta := map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()}
	if format != nil {
		meta[mqttStoreFormatMetaKey] = *format
	}
	return &StreamInfo{Config: StreamConfig{Name: name, Metadata: meta}}
}

func testStrPtr(s string) *string { return &s }

// TestEnsureBrokerStreamRefusesAForeignStoreFormat: the format verdict is taken on
// the authoritative re-read and BEFORE the validator. The validator here admits
// everything and records that it ran, so a check moved after it — or dropped — is
// what fails, not a shape the stub happens to reject.
func TestEnsureBrokerStreamRefusesAForeignStoreFormat(t *testing.T) {
	t.Parallel()
	notFound := func() (*StreamInfo, error) { return nil, ApiErrors[JSStreamNotFoundErr] }
	cfg := &StreamConfig{Name: "$MQTT_test"}

	for _, tc := range []struct {
		name   string
		format *string
		// otherFormat: the refusal must offer the matching release before any reset.
		otherFormat bool
	}{
		{"no stamp", nil, false},
		{"an empty stamp", testStrPtr(""), false},
		{"another format", testStrPtr("2"), true},
	} {
		check := func(t *testing.T, err error, validated int) {
			t.Helper()
			if err == nil {
				t.Fatalf("a stream with %s must be refused", tc.name)
			}
			if validated != 0 {
				t.Fatalf("the validator ran %d times: the format verdict must precede the shape", validated)
			}
			for _, want := range []string{mqttStoreFormatMetaKey, fmt.Sprintf("%q", mqttStoreFormat)} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal must name %s, got: %v", want, err)
				}
			}
			if tc.otherFormat {
				msg := err.Error()
				run, del := strings.Index(msg, "Run a broker release"), strings.Index(msg, "delete ALL")
				if run < 0 || del < 0 || run > del {
					t.Fatalf("another format's refusal must offer the matching release before the reset: %v", err)
				}
			}
		}

		t.Run(tc.name+": pre-existing stream", func(t *testing.T) {
			info := testFormatInfo("B", tc.format)
			stub := &stubStreamAPI{lookupResults: []func() (*StreamInfo, error){
				func() (*StreamInfo, error) { return info, nil },
				func() (*StreamInfo, error) { return info, nil },
			}}
			validated := 0
			_, err := ensureBrokerStream(stub, cfg, func(*StreamInfo) error { validated++; return nil }, nil, nil)
			check(t, err, validated)
			if stub.creates != 0 {
				t.Fatalf("a refused stream must not be re-created (creates=%d)", stub.creates)
			}
		})

		t.Run(tc.name+": create-race loser re-reads the winner's stream", func(t *testing.T) {
			info := testFormatInfo("B", tc.format)
			stub := &stubStreamAPI{
				lookupResults: []func() (*StreamInfo, error){
					notFound,
					func() (*StreamInfo, error) { return info, nil },
				},
				createResult: func() (*StreamInfo, bool, error) { return nil, false, ApiErrors[JSStreamNameExistErr] },
			}
			validated := 0
			_, err := ensureBrokerStream(stub, cfg, func(*StreamInfo) error { validated++; return nil }, nil, nil)
			check(t, err, validated)
		})
	}

	t.Run("this build's format is admitted to the validator", func(t *testing.T) {
		info := testFormatInfo("B", testStrPtr(mqttStoreFormat))
		stub := &stubStreamAPI{
			lookupResults: []func() (*StreamInfo, error){
				notFound,
				func() (*StreamInfo, error) { return info, nil },
			},
			createResult: func() (*StreamInfo, bool, error) { return info, true, nil },
		}
		validated := 0
		si, err := ensureBrokerStream(stub, cfg, func(*StreamInfo) error { validated++; return nil }, nil, nil)
		if err != nil || si != info || validated != 1 {
			t.Fatalf("a stream in this build's format must reach the validator once and be admitted (si=%v err=%v validated=%d)", si, err, validated)
		}
	})
}

// TestBrokerStreamSpecTableStampsTheStoreFormat: every stream the broker creates carries
// the persisted format beside the account's epoch, so a bundle this build provisions
// is one its own format check admits.
func TestBrokerStreamSpecTableStampsTheStoreFormat(t *testing.T) {
	t.Parallel()
	specs := testBrokerStreamSpecs()
	epoch := specs[0].cfg.Metadata[mqttSessEpochMetaKey]
	if !mqttValidSessEpoch(epoch) {
		t.Fatalf("%s minted no valid epoch: %q", mqttSessStreamName, epoch)
	}
	for _, spec := range specs {
		if got := spec.cfg.Metadata[mqttStoreFormatMetaKey]; got != mqttStoreFormat {
			t.Errorf("%s is created with %q = %q, want %q", spec.name, mqttStoreFormatMetaKey, got, mqttStoreFormat)
		}
		if got := spec.cfg.Metadata[mqttSessEpochMetaKey]; got != epoch {
			t.Errorf("%s is created with epoch %q, want the bundle's %q", spec.name, got, epoch)
		}
		if err := mqttRefuseForeignStoreFormat(spec.cfg); err != nil {
			t.Errorf("the format check refuses %s as this build creates it: %v", spec.name, err)
		}
	}
}

// TestBrokerStreamBundlePreflightRefusesAForeignFormatMember: a member in another
// format is refused on the FIRST scan, before the count rule, naming every such
// member. With a budget long enough that a rescan would show, a partial vector that
// fell through to the count rule would wait and then refuse as partial.
func TestBrokerStreamBundlePreflightRefusesAForeignFormatMember(t *testing.T) {
	t.Parallel()
	specs := testBundleSpecs()
	names := mqttBrokerStreamNames(specs)

	for _, tc := range []struct {
		name    string
		present []string
		foreign map[string]*string
		// wantOther: the refusal is the other-format arm (a release to run).
		wantOther bool
	}{
		{"a partial bundle in another format", names[:3], map[string]*string{names[0]: testStrPtr("2"), names[2]: testStrPtr("2")}, true},
		{"a partial unstamped bundle", names[3:6], map[string]*string{names[3]: nil, names[4]: nil, names[5]: nil}, false},
		{"a complete bundle with one member in another format", names, map[string]*string{names[len(names)-1]: testStrPtr("9")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &testBundleLookupStub{info: map[string]*StreamInfo{}}
			for _, n := range tc.present {
				if f, ok := tc.foreign[n]; ok {
					st.info[n] = testFormatInfo(n, f)
				} else {
					st.info[n] = testPresentInfo(n)
				}
			}
			start := time.Now()
			err := mqttPreflightBrokerStreamBundle(st, specs, 10*time.Second, nil, nil)
			if err == nil {
				t.Fatal("a member in another persisted format must be refused")
			}
			if len(st.seen) != len(specs) {
				t.Fatalf("the refusal took %d lookups, want exactly one scan of %d", len(st.seen), len(specs))
			}
			if time.Since(start) > 5*time.Second {
				t.Fatalf("the refusal waited on the rescans: %v", err)
			}
			if strings.Contains(err.Error(), "broker-owned streams — present:") {
				t.Fatalf("the refusal is the count rule's, not the format check's: %v", err)
			}
			for n, f := range tc.foreign {
				v := ""
				if f != nil {
					v = *f
				}
				if !strings.Contains(err.Error(), fmt.Sprintf("%s=%q", n, v)) {
					t.Errorf("the refusal does not name %s with its stamp %q: %v", n, v, err)
				}
			}
			if got := strings.Contains(err.Error(), "Run a broker release"); got != tc.wantOther {
				t.Fatalf("other-format guidance = %v, want %v: %v", got, tc.wantOther, err)
			}
		})
	}
}
