package requestreply

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/sllt/watermill"
	"github.com/sllt/watermill/message"
)

type BackendPubsubMarshaler[Result any] interface {
	MarshalReply(params BackendOnCommandProcessedParams[Result]) (*message.Message, error)
	UnmarshalReply(msg *message.Message) (reply Reply[Result], err error)
}

const (
	ErrorMetadataKey    = "_watermill_requestreply_error"
	HasErrorMetadataKey = "_watermill_requestreply_has_error"
)

type BackendPubsubJSONMarshaler[Result any] struct{}

func (m BackendPubsubJSONMarshaler[Result]) MarshalReply(
	params BackendOnCommandProcessedParams[Result],
) (*message.Message, error) {
	msg := message.NewMessage(watermill.NewUUID(), nil)

	if params.HandleErr != nil {
		msg.Metadata.Set(ErrorMetadataKey, params.HandleErr.Error())
		msg.Metadata.Set(HasErrorMetadataKey, "1")
	} else {
		msg.Metadata.Set(HasErrorMetadataKey, "0")
	}

	b, err := json.Marshal(params.HandlerResult)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal reply")
	}
	msg.Payload = b

	return msg, nil
}

func (m BackendPubsubJSONMarshaler[Result]) UnmarshalReply(msg *message.Message) (Reply[Result], error) {
	reply := Reply[Result]{}

	if msg.Metadata.Get(HasErrorMetadataKey) == "1" {
		reply.Error = errors.New(msg.Metadata.Get(ErrorMetadataKey))
	}

	var result Result
	if err := json.Unmarshal(msg.Payload, &result); err != nil {
		return Reply[Result]{}, errors.Wrap(err, "cannot unmarshal result")
	}
	reply.HandlerResult = result

	return reply, nil
}
