package codec

// SGd AVP codes per 3GPP TS 29.338 (Vendor-ID 10415 unless noted)
const (
	// Base (no vendor) AVPs used in SGd
	CodeUserIdentifier                   uint32 = 3102 // Grouped (vendor 10415)
	CodeSCAddress                        uint32 = 3300 // OctetString (vendor 10415)
	CodeSMRPUI                           uint32 = 3301 // OctetString — RP-DATA (vendor 10415)
	CodeTFRFlags                         uint32 = 3302 // Unsigned32 (vendor 10415)
	CodeSMDeliveryFailureCause           uint32 = 3303 // Grouped (vendor 10415)
	CodeSMEnumeratedDeliveryFailureCause uint32 = 3304 // Enumerated (vendor 10415)
	CodeSMDeliveryTimer                  uint32 = 3306 // Unsigned32 (vendor 10415)
	CodeSMDeliveryStartTime              uint32 = 3307 // Time (vendor 10415)
	CodeSMRPMTI                          uint32 = 3308 // Enumerated (vendor 10415)
	CodeSMRPSMEA                         uint32 = 3309 // OctetString (vendor 10415)
	CodeMWDStatus                        uint32 = 3312 // Unsigned32 (vendor 10415) bitmask
	CodeSMDeliveryOutcome                uint32 = 3316 // Grouped (vendor 10415)
	CodeSMSGWDeliveryOutcome             uint32 = 3320 // Grouped IP-SM-GW-SM-Delivery-Outcome (vendor 10415)
	CodeOFRFlags                         uint32 = 3328 // Unsigned32 (vendor 10415)
	CodeMaximumUEAvailability            uint32 = 3329 // Time (vendor 10415)
	CodeMaximumRetransmission            uint32 = 3330 // Time (vendor 10415)
	CodeRequestedRetransmTime            uint32 = 3331 // Time (vendor 10415)

	// Re-used 3GPP AVPs referenced by SGd procedures.
	CodeMMENumberForMTSMS uint32 = 1607 // OctetString, TS 29.272
	CodeSGSNNumber        uint32 = 1606 // OctetString, TS 29.272

	// MSISDN AVP (used in SGd for subscriber identification)
	CodeMSISDN uint32 = 701 // OctetString, BCD-encoded (vendor 10415)

	// SM-RP-MTI enumerated values
	SMRPMTISubmit        = 0 // SMS-SUBMIT (MO from UE)
	SMRPMTIDeliver       = 1 // SMS-DELIVER (MT to UE)
	SMRPMTIDeliverReport = 2

	// SMS-GW-Delivery-Outcome enumerated values
	SMSGWSuccessful       = 0
	SMSGWAbsentSubscriber = 1
	SMSGWOtherError       = 2
)
