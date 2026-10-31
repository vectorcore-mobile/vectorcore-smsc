package sgd

import (
	"fmt"

	"github.com/svinson1121/vectorcore-smsc/internal/codec"
	"github.com/svinson1121/vectorcore-smsc/internal/codec/tpdu"
	dcodec "github.com/svinson1121/vectorcore-smsc/internal/diameter/codec"
)

// EncodeOFR builds the AVPs for an OFR (MT-Forward-Short-Message-Request).
// The caller builds the full Diameter request and adds the returned AVPs.
func EncodeOFR(msg *codec.Message, scAddr, scAddrEncoding string) ([]*dcodec.AVP, error) {
	return encodeForwardSM(msg, scAddr, scAddrEncoding, MSISDNEncodingTBCD, dcodec.SMRPMTIDeliver)
}

// EncodeTFR builds the AVPs for a TFR (MO-Forward-Short-Message-Request).
func EncodeTFR(msg *codec.Message, scAddr, scAddrEncoding string, msisdnEncoding MSISDNEncoding) ([]*dcodec.AVP, error) {
	return encodeForwardSM(msg, scAddr, scAddrEncoding, msisdnEncoding, dcodec.SMRPMTISubmit)
}

func encodeForwardSM(msg *codec.Message, scAddr, scAddrEncoding string, msisdnEncoding MSISDNEncoding, mti int) ([]*dcodec.AVP, error) {
	// Encode the canonical message to TP-DATA
	var tpData []byte
	var err error
	if mti == dcodec.SMRPMTIDeliver {
		tpData, err = tpdu.EncodeMT(msg)
	} else {
		tpData, err = tpdu.EncodeSubmit(msg)
	}
	if err != nil {
		return nil, fmt.Errorf("sgd encode: tpdu: %w", err)
	}

	avps := make([]*dcodec.AVP, 0, 5)

	if mti == dcodec.SMRPMTIDeliver {
		if msg.Destination.IMSI == "" {
			return nil, fmt.Errorf("sgd encode: destination IMSI required for MT")
		}
		avps = append(avps, dcodec.NewString(dcodec.CodeUserName, 0, dcodec.FlagMandatory, msg.Destination.IMSI))
		if msg.Destination.MMENumber != "" {
			avps = append(avps, dcodec.NewOctetString(
				dcodec.CodeMMENumberForMTSMSServing,
				dcodec.Vendor3GPP,
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
				encodeBCDDigits(msg.Destination.MMENumber),
			))
		}
	}

	avps = append(avps,
		// SM-RP-UI: raw TP-DATA (vendor 10415)
		dcodec.NewOctetString(dcodec.CodeSMRPUI, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific, tpData),
	)

	// SC-Address (vendor 10415) — raw TBCD digits, international format.
	scAVP := dcodec.NewOctetString(dcodec.CodeSCAddress, dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific, encodeSCAddress(scAddr, scAddrEncoding))
	avps = append(avps, scAVP)

	// MO over SGd can identify the UE directly via MSISDN.
	if mti != dcodec.SMRPMTIDeliver && msg.Destination.MSISDN != "" {
		msisdnAVP := dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
			EncodeMSISDN(msg.Destination.MSISDN, msisdnEncoding))
		avps = append(avps, msisdnAVP)
	}

	return avps, nil
}

// EncodeRSR builds AVPs for a Report-SM-Delivery-Status-Request (RSR).
// outcome: 0=success, 1=absent, 2=other-error (SMSGWDeliveryOutcome values)
func EncodeRSR(msisdn, scAddr, scAddrEncoding string, msisdnEncoding MSISDNEncoding, outcome uint32) ([]*dcodec.AVP, error) {
	smOutcome, err := dcodec.NewGrouped(
		dcodec.CodeSMDeliveryOutcome, dcodec.Vendor3GPP,
		dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
		[]*dcodec.AVP{
			dcodec.NewUint32(dcodec.CodeSMSGWDeliveryOutcome, dcodec.Vendor3GPP,
				dcodec.FlagMandatory|dcodec.FlagVendorSpecific, outcome),
		},
	)
	if err != nil {
		return nil, err
	}

	avps := []*dcodec.AVP{
		dcodec.NewOctetString(dcodec.CodeMSISDN, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
			EncodeMSISDN(msisdn, msisdnEncoding)),
		dcodec.NewOctetString(dcodec.CodeSCAddress, dcodec.Vendor3GPP,
			dcodec.FlagMandatory|dcodec.FlagVendorSpecific,
			encodeSCAddress(scAddr, scAddrEncoding)),
		smOutcome,
	}
	return avps, nil
}
