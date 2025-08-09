package topics

type Topic string

const (
	WHATSAPP_MESSAGES Topic = "whatsapp_messages"
	WHATSAPP_STATUS   Topic = "whatsapp_status"
	PIX_CONFIRMATIONS Topic = "pix_confirmations"
	PIX_NEW_CHARGES   Topic = "pix_new_charges"
)
