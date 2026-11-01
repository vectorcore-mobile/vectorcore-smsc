package forwarder

import (
	"context"
	"errors"

	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
	diametersgd "github.com/svinson1121/vectorcore-smsc/internal/diameter/sgd"
	"github.com/svinson1121/vectorcore-smsc/internal/store"
)

// failureReporter is implemented by reporters that can carry the cause of a
// final delivery failure into the report sent to the originator (the TP-ST
// of an SMS-STATUS-REPORT).
type failureReporter interface {
	ReportFailure(ctx context.Context, m store.Message, status string, tpStatus byte)
}

// reportFailed reports a message that has failed permanently. lastErr is the
// error of the last delivery attempt; when it identifies the cause, the
// report carries a matching TP-ST instead of the generic one.
func (f *Forwarder) reportFailed(ctx context.Context, messageID string, lastErr error) {
	if f.reporter == nil {
		return
	}
	stored, err := f.st.GetMessage(ctx, messageID)
	if err != nil || stored == nil {
		return
	}
	if fr, ok := f.reporter.(failureReporter); ok {
		if st, ok := tpStatusForFailure(lastErr); ok {
			fr.ReportFailure(ctx, *stored, "FAILED", st)
			return
		}
	}
	f.reporter.Report(ctx, *stored, "FAILED")
}

// tpStatusForFailure maps the error of the last delivery attempt to a final
// TP-ST (3GPP TS 23.040 §9.2.3.15). Errors that were retried until the retry
// budget ran out map to "temporary error, SC is not making any more transfer
// attempts" (0x6X); definitive refusals map to permanent errors (0x4X).
// ok is false when the error carries no usable cause.
func tpStatusForFailure(err error) (st byte, ok bool) {
	var ofaErr *diametersgd.OFAResultError
	if !errors.As(err, &ofaErr) {
		return 0, false
	}
	if !ofaErr.Experimental {
		switch ofaErr.ResultCode {
		case dcodec.DiameterUnableToDeliver:
			return tpdu.STFinalNoResponseFromSME, true
		case dcodec.DiameterTooBusy:
			return tpdu.STFinalCongestion, true
		}
		return 0, false
	}
	switch ofaErr.ResultCode {
	case diametersgd.ResultErrorAbsentUser:
		return tpdu.STFinalNoResponseFromSME, true
	case diametersgd.ResultErrorUserBusyForMTSMS:
		return tpdu.STFinalSMEBusy, true
	case diametersgd.ResultErrorFacilityNotSupported:
		return tpdu.STPermNoInterworking, true
	case diametersgd.ResultErrorIllegalUser:
		return tpdu.STPermNotObtainable, true
	case diametersgd.ResultErrorIllegalEquipment:
		return tpdu.STPermIncompatibleDestination, true
	case diametersgd.ResultErrorSMDeliveryFailure:
		if ofaErr.DeliveryFailureCause == nil {
			return tpdu.STPermRemoteProcedureError, true
		}
		switch *ofaErr.DeliveryFailureCause {
		case diametersgd.DeliveryFailureMemoryCapacityExceeded:
			return tpdu.STFinalErrorInSME, true
		case diametersgd.DeliveryFailureEquipmentProtocolError:
			return tpdu.STPermRemoteProcedureError, true
		case diametersgd.DeliveryFailureEquipmentNotSMEquipped:
			return tpdu.STPermIncompatibleDestination, true
		case diametersgd.DeliveryFailureSCCongestion:
			return tpdu.STFinalCongestion, true
		case diametersgd.DeliveryFailureInvalidSMEAddress:
			return tpdu.STPermNotObtainable, true
		case diametersgd.DeliveryFailureUnknownServiceCentre,
			diametersgd.DeliveryFailureUserNotSCUser:
			return tpdu.STFinalServiceRejected, true
		}
		return tpdu.STPermRemoteProcedureError, true
	}
	return 0, false
}
