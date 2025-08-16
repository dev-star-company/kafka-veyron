package kafka_veyron

import (
	"encoding/json"
	"time"

	"github.com/dev-star-company/kafka-veyron/topics"
	"github.com/segmentio/kafka-go"
)

type PixRecebimento struct {
	EndToEndId  string          `json:"endToEndId"`
	Valor       string          `json:"valor"`
	Horario     time.Time       `json:"horario"`
	InfoPagador string          `json:"infoPagador"`
	NomePagador string          `json:"nomePagador"`
	Pagador     Pagador `json:"pagador"`
	Devolucoes  []PixDevolucao  `json:"devolucoes"`
	Banco       string          `json:"banco"`
	ClientID    string          `json:"clientId"`
}

type PixDevolucao struct {
	Id      string              `json:"id"`
	RtrId   string              `json:"rtrId"`
	Valor   string              `json:"valor"`
	Horario PixDevolucaoHorario `json:"horario"`
	Status  string              `json:"status"`
	Motivo  string              `json:"motivo"`
}

type PixDevolucaoHorario struct {
	Solicitacao time.Time `json:"solicitacao"`
	Liquidacao  *time.Time `json:"liquidacao"`
}

type Pagador struct {
	Nome string  `json:"nome"`
	CPF  *string `json:"cpf"`
	CNPJ *string `json:"cnpj"`
}

func (p *KafkaVeyroner) PublishToPixConfirmation(message Message[PixRecebimento]) error {
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
