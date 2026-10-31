package dr

import (
	"context"
	"testing"
	"time"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	"github.com/svinson1121/vectorcore-smsc/internal/numbering"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// drTestStore implements only what Report touches; any other call panics.
type drTestStore struct {
	store.Store
	reports []store.DeliveryReport
}

func (s *drTestStore) SaveDeliveryReport(_ context.Context, dr store.DeliveryReport) error {
	s.reports = append(s.reports, dr)
	return nil
}

type captureDispatcher struct {
	msgs chan *codec.Message
}

func newCaptureDispatcher() *captureDispatcher {
	return &captureDispatcher{msgs: make(chan *codec.Message, 4)}
}

func (d *captureDispatcher) Dispatch(_ context.Context, msg *codec.Message) { d.msgs <- msg }

func (d *captureDispatcher) expectOne(t *testing.T) *codec.Message {
	t.Helper()
	select {
	case msg := <-d.msgs:
		return msg
	case <-time.After(time.Second):
		t.Fatal("expected a status report to be dispatched")
		return nil
	}
}

func (d *captureDispatcher) expectNone(t *testing.T) {
	t.Helper()
	select {
	case msg := <-d.msgs:
		t.Fatalf("unexpected dispatch: %+v", msg)
	case <-time.After(100 * time.Millisecond):
	}
}

func newStatusReportCorrelator() (*Correlator, *drTestStore, *captureDispatcher) {
	st := &drTestStore{}
	d := newCaptureDispatcher()
	c := New(st, nil, nil, "+15550000000")
	c.SetDispatcher(d)
	return c, st, d
}

func originalSGdMessage() store.Message {
	mr := 0x2A
	delivered := time.Date(2026, 10, 30, 17, 52, 45, 0, time.UTC)
	return store.Message{
		ID:          "orig-1",
		TPMR:        &mr,
		OriginIface: "sgd",
		OriginPeer:  "mme-a.epc.example.net",
		EgressIface: "sgd",
		SrcMSISDN:   "989100000555",
		DstMSISDN:   "989100000111",
		DRRequired:  true,
		SubmittedAt: time.Date(2026, 10, 30, 17, 52, 38, 0, time.UTC),
		DeliveredAt: &delivered,
	}
}

func TestReportDispatchesStatusReportToOriginalSender(t *testing.T) {
	c, st, d := newStatusReportCorrelator()
	orig := originalSGdMessage()

	c.Report(context.Background(), orig, StatusDelivered)
	sr := d.expectOne(t)

	if sr.Kind != codec.MessageKindStatusReport {
		t.Errorf("Kind = %q, want status_report", sr.Kind)
	}
	if sr.RefMessageID != orig.ID {
		t.Errorf("RefMessageID = %q, want %q", sr.RefMessageID, orig.ID)
	}
	if sr.ID == "" || sr.ID == orig.ID {
		t.Errorf("ID = %q, want a new message ID", sr.ID)
	}
	if sr.IngressInterface != codec.InterfaceInternal {
		t.Errorf("IngressInterface = %q, want internal", sr.IngressInterface)
	}
	if sr.Destination.MSISDN != "989100000555" || sr.Destination.TON != numbering.TONInternational {
		t.Errorf("Destination = %+v, want original sender 989100000555 (international)", sr.Destination)
	}
	if sr.Source.MSISDN != "15550000000" {
		t.Errorf("Source.MSISDN = %q, want SC address 15550000000", sr.Source.MSISDN)
	}
	if sr.TPSRRequired {
		t.Error("status report must not itself request a status report")
	}

	decoded, err := tpdu.Decode(sr.RawTPDU)
	if err != nil {
		t.Fatalf("decode RawTPDU: %v", err)
	}
	if decoded.TPMR != 0x2A {
		t.Errorf("TP-MR = 0x%02X, want 0x2A (original)", decoded.TPMR)
	}
	if decoded.Source.MSISDN != "989100000111" {
		t.Errorf("TP-RA = %q, want original recipient 989100000111", decoded.Source.MSISDN)
	}
	if decoded.TPStatus != tpdu.STReceivedBySME {
		t.Errorf("TP-ST = 0x%02X, want 0x00", decoded.TPStatus)
	}
	if !decoded.Timestamp.Equal(orig.SubmittedAt) {
		t.Errorf("TP-SCTS = %v, want submitted_at %v", decoded.Timestamp, orig.SubmittedAt)
	}
	if !decoded.DischargeTime.Equal(*orig.DeliveredAt) {
		t.Errorf("TP-DT = %v, want delivered_at %v", decoded.DischargeTime, *orig.DeliveredAt)
	}

	if len(st.reports) != 1 || st.reports[0].MessageID != orig.ID {
		t.Fatalf("delivery reports = %+v, want one for %s", st.reports, orig.ID)
	}
}

func TestReportMapsFinalStatusToTPStatus(t *testing.T) {
	cases := []struct {
		status string
		want   byte
	}{
		{StatusDelivered, tpdu.STReceivedBySME},
		{StatusExpired, tpdu.STPermValidityPeriodExpired},
		{StatusFailed, tpdu.STPermNotObtainable},
		{StatusUndeliv, tpdu.STPermNotObtainable},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			c, _, d := newStatusReportCorrelator()
			before := time.Now().UTC().Add(-time.Second)

			c.Report(context.Background(), originalSGdMessage(), tc.status)
			decoded, err := tpdu.Decode(d.expectOne(t).RawTPDU)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if decoded.TPStatus != tc.want {
				t.Fatalf("TP-ST = 0x%02X, want 0x%02X", decoded.TPStatus, tc.want)
			}
			// For anything but delivery, TP-DT is when the SC gave up (now).
			if tc.status != StatusDelivered && decoded.DischargeTime.Before(before) {
				t.Fatalf("TP-DT = %v, want approximately now", decoded.DischargeTime)
			}
		})
	}
}

func TestReportDispatchesStatusReportForSIP3GPPOrigin(t *testing.T) {
	c, _, d := newStatusReportCorrelator()
	orig := originalSGdMessage()
	orig.OriginIface = "sip3gpp"

	c.Report(context.Background(), orig, StatusDelivered)
	d.expectOne(t)
}

func TestReportSkipsStatusReport(t *testing.T) {
	cases := map[string]func(*store.Message){
		"sender did not request one (TP-SRR=0)": func(m *store.Message) { m.DRRequired = false },
		"SMPP origin uses deliver_sm receipt":   func(m *store.Message) { m.OriginIface = "smpp"; m.OriginPeer = "" },
		"SIP SIMPLE origin":                     func(m *store.Message) { m.OriginIface = "sipsimple" },
		"no TP-MR recorded":                     func(m *store.Message) { m.TPMR = nil },
		"no sender MSISDN":                      func(m *store.Message) { m.SrcMSISDN = "" },
		// Loop guard: the report's own outcome must never produce another report.
		"message is itself a status report": func(m *store.Message) {
			m.Kind = store.MessageKindStatusReport
			m.RefMessageID = "orig-0"
			m.OriginIface = "internal"
		},
		// Even if a status report row were tagged with a TPDU origin.
		"status report with TPDU origin": func(m *store.Message) {
			m.Kind = store.MessageKindStatusReport
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c, st, d := newStatusReportCorrelator()
			m := originalSGdMessage()
			mutate(&m)

			c.Report(context.Background(), m, StatusDelivered)
			d.expectNone(t)
			if len(st.reports) != 1 {
				t.Fatalf("delivery reports = %d, want 1 (outcome is still recorded)", len(st.reports))
			}
		})
	}
}

func TestReportWithoutDispatcherDoesNotPanic(t *testing.T) {
	c := New(&drTestStore{}, nil, nil, "+15550000000")
	c.Report(context.Background(), originalSGdMessage(), StatusDelivered)
}
