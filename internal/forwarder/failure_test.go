package forwarder

import (
	"errors"
	"fmt"
	"testing"

	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
	diametersgd "github.com/svinson1121/vectorcore-smsc/internal/diameter/sgd"
)

func TestTPStatusForFailure(t *testing.T) {
	cause := func(v uint32) *uint32 { return &v }
	exp := func(rc uint32, c *uint32) error {
		return &diametersgd.OFAResultError{ResultCode: rc, Experimental: true, DeliveryFailureCause: c}
	}
	cases := []struct {
		name   string
		err    error
		wantST byte
		wantOK bool
	}{
		{"absent user", exp(diametersgd.ResultErrorAbsentUser, nil), tpdu.STFinalNoResponseFromSME, true},
		{"busy", exp(diametersgd.ResultErrorUserBusyForMTSMS, nil), tpdu.STFinalSMEBusy, true},
		{"facility not supported", exp(diametersgd.ResultErrorFacilityNotSupported, nil), tpdu.STPermNoInterworking, true},
		{"illegal user", exp(diametersgd.ResultErrorIllegalUser, nil), tpdu.STPermNotObtainable, true},
		{"illegal equipment", exp(diametersgd.ResultErrorIllegalEquipment, nil), tpdu.STPermIncompatibleDestination, true},
		{"memory full", exp(diametersgd.ResultErrorSMDeliveryFailure, cause(diametersgd.DeliveryFailureMemoryCapacityExceeded)), tpdu.STFinalErrorInSME, true},
		{"protocol error", exp(diametersgd.ResultErrorSMDeliveryFailure, cause(diametersgd.DeliveryFailureEquipmentProtocolError)), tpdu.STPermRemoteProcedureError, true},
		{"not SM equipped", exp(diametersgd.ResultErrorSMDeliveryFailure, cause(diametersgd.DeliveryFailureEquipmentNotSMEquipped)), tpdu.STPermIncompatibleDestination, true},
		{"unknown SC", exp(diametersgd.ResultErrorSMDeliveryFailure, cause(diametersgd.DeliveryFailureUnknownServiceCentre)), tpdu.STFinalServiceRejected, true},
		{"SC congestion", exp(diametersgd.ResultErrorSMDeliveryFailure, cause(diametersgd.DeliveryFailureSCCongestion)), tpdu.STFinalCongestion, true},
		{"invalid SME address", exp(diametersgd.ResultErrorSMDeliveryFailure, cause(diametersgd.DeliveryFailureInvalidSMEAddress)), tpdu.STPermNotObtainable, true},
		{"not SC user", exp(diametersgd.ResultErrorSMDeliveryFailure, cause(diametersgd.DeliveryFailureUserNotSCUser)), tpdu.STFinalServiceRejected, true},
		{"delivery failure without cause", exp(diametersgd.ResultErrorSMDeliveryFailure, nil), tpdu.STPermRemoteProcedureError, true},
		{"3002 unable to deliver", &diametersgd.OFAResultError{ResultCode: dcodec.DiameterUnableToDeliver}, tpdu.STFinalNoResponseFromSME, true},
		{"3004 too busy", &diametersgd.OFAResultError{ResultCode: dcodec.DiameterTooBusy}, tpdu.STFinalCongestion, true},
		{"wrapped", fmt.Errorf("deliver: %w", exp(diametersgd.ResultErrorIllegalEquipment, nil)), tpdu.STPermIncompatibleDestination, true},
		{"other base result", &diametersgd.OFAResultError{ResultCode: dcodec.DiameterUnableToComply}, 0, false},
		{"unknown experimental", exp(5999, nil), 0, false},
		{"not an SGd error", errors.New("S6c lookup failed"), 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range cases {
		st, ok := tpStatusForFailure(tc.err)
		if ok != tc.wantOK || st != tc.wantST {
			t.Errorf("%s: tpStatusForFailure() = 0x%02X, %v; want 0x%02X, %v", tc.name, st, ok, tc.wantST, tc.wantOK)
		}
	}
}
