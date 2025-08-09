package kafka_veyron

import (
	"errors"
	"time"

	"github.com/dev-star-company/custom-validate/validate"
)

// WIP
type PixNewCharges struct {
	TransactionId string
	Bank          string
	// ReferenceId as generic name to extra id that some banks use, like end to end id
	ReferenceId    *string
	QrCode         *string
	CopyNPasteCode *string
	Amount         float64
	At             time.Time
}

func NewPixNewCharges(transactionId, bank string, amount float64, at time.Time, referenceId *string) (*PixNewCharges, error) {
	fields := map[string]string{
		"TransactionId": "required",
		"Bank":          "required",
		"Amount":        "required,gt=0",
		"At":            "required",
	}

	msg := &PixNewCharges{
		TransactionId: transactionId,
		Bank:          bank,
		Amount:        amount,
		At:            at,
		ReferenceId:   referenceId,
	}

	if err := validate.Validate(fields, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func (p *KafkaVeyroner) PublishToPixNewCharges(message Message[PixNewCharges]) error {
	return errors.New("WIP, do not use yet")
	// conn, err := p.Connect(topics.PIX_NEW_CHARGES)
	// if err != nil {
	// 	return err
	// }

	// msgBytes, err := json.Marshal(message)
	// if err != nil {
	// 	return err
	// }
	// conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	// _, err = conn.WriteMessages(
	// 	kafka.Message{Value: msgBytes},
	// )
	// if err != nil {
	// 	return err
	// }
	// if err := conn.Close(); err != nil {
	// 	return err
	// }

	// return nil
}
