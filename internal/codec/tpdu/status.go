package tpdu

// TP-Status values for SMS-STATUS-REPORT (3GPP TS 23.040 §9.2.3.15).
const (
	// Short message transaction completed
	STReceivedBySME        byte = 0x00 // short message received by the SME
	STForwardedUnconfirmed byte = 0x01 // forwarded by the SC to the SME, delivery unconfirmed
	STReplacedBySC         byte = 0x02 // short message replaced by the SC

	// Temporary error, SC still trying to transfer SM
	STTempCongestion        byte = 0x20
	STTempSMEBusy           byte = 0x21
	STTempNoResponseFromSME byte = 0x22
	STTempServiceRejected   byte = 0x23
	STTempQoSNotAvailable   byte = 0x24
	STTempErrorInSME        byte = 0x25

	// Permanent error, SC is not making any more transfer attempts
	STPermRemoteProcedureError    byte = 0x40
	STPermIncompatibleDestination byte = 0x41
	STPermConnectionRejectedBySME byte = 0x42
	STPermNotObtainable           byte = 0x43
	STPermQoSNotAvailable         byte = 0x44
	STPermNoInterworking          byte = 0x45
	STPermValidityPeriodExpired   byte = 0x46
	STPermDeletedByOriginator     byte = 0x47
	STPermDeletedBySCAdmin        byte = 0x48
	STPermSMDoesNotExist          byte = 0x49

	// Temporary error, SC is not making any more transfer attempts
	STFinalCongestion        byte = 0x60
	STFinalSMEBusy           byte = 0x61
	STFinalNoResponseFromSME byte = 0x62
	STFinalServiceRejected   byte = 0x63
	STFinalQoSNotAvailable   byte = 0x64
	STFinalErrorInSME        byte = 0x65
)
