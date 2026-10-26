//go:build mqtt_fulltests

package mqtt

// testReadyForUse is the durable and configuration half of readiness for use, for a fixture that
// holds only a resource's (generation, incarnation): the resource, the ownership of the resource's
// own name and stream (client-checked), and the stream's inactivity limit, each judged by the
// judgment the admission's readiness rounds apply. The consumer's own usability is not part of it.
func testReadyForUse(jsa *mqttJSA, id, epoch, gen, inc string) error {
	r, _, rerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	stream, err := mqttJudgeReadyResource(gen, inc, r, rerr)
	if err != nil {
		return err
	}
	o, _, oerr := jsa.loadNameOwnership(id, epoch, stream, r.Name)
	if err := mqttJudgeReadyOwnership(gen, inc, r.Name, o, oerr); err != nil {
		return err
	}
	streamName, serr := mqttStreamForOwned(stream)
	if serr != nil {
		return serr
	}
	si, lerr := jsa.lookupStream(streamName)
	return mqttJudgeReadyStream(gen, inc, streamName, si, lerr)
}
