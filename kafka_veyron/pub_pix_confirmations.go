package kafka_veyron

import (
	"encoding/json"
	"time"

	"github.com/dev-star-company/custom-validate/validate"
	"github.com/dev-star-company/kafka-veyron/topics"
	"github.com/segmentio/kafka-go"
)

type PixConfirmation struct {
	TransactionId string
	Bank          string
	Amount        float64
	At            time.Time
	ReferenceId   *string
}

func NewPixConfirmation(transactionId, bank string, amount float64, at time.Time, referenceId *string) (*PixConfirmation, error) {
	fields := map[string]string{
		"TransactionId": "required",
		"Bank":          "required",
		"Amount":        "required,gt=0",
		"At":            "required",
	}

	msg := &PixConfirmation{
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

func (p *KafkaVeyroner) PublishToPixConfirmation(message Message[PixConfirmation]) error {
	conn, err := p.Connect(topics.PIX_CONFIRMATIONS)
	if err != nil {
		return err
	}

	msgBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: msgBytes},
	)
	if err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}

	return nil
}
