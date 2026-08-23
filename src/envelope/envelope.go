package envelope

type Header struct {
	ID    EnvelopeId
	Topic string
	From  ActorId
	Cause EnvelopeId
}

type Envelope struct {
	Header  Header
	Payload any
}

type Options struct {
	Cause EnvelopeId
	ID    EnvelopeId
	IDGen IdGen
}

func MakeEnvelope(topic string, payload any, from ActorId, opt *Options) (*Envelope, error) {
	if _, err := SplitTopic(topic); err != nil {
		return nil, err
	}
	if opt == nil {
		opt = &Options{}
	}
	var eid EnvelopeId
	if opt.ID != "" {
		eid = opt.ID
	} else if opt.IDGen != nil {
		eid = opt.IDGen.NextEnvelopeID()
	} else {
		return nil, &EnvelopeBuildError{Msg: "make_envelope requires id or id_gen"}
	}
	cause := eid
	if opt.Cause != "" {
		cause = opt.Cause
	}
	return &Envelope{
		Header:  Header{ID: eid, Topic: topic, From: from, Cause: cause},
		Payload: payload,
	}, nil
}
