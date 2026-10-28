// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A subscription's QoS 1/2 offers that are already queued are read together, and their fresh
// deliveries admitted in one batch.

package mqtt

// mqttHeldOffer is one QoS 1/2 offer a subscription's delivery callback holds: everything its
// processing reads, owned. The header length is the offer's own — the per-subscription delivery
// client's is overwritten for every message the connection layer dispatches.
type mqttHeldOffer struct {
	subject string
	reply   string
	rmsg    []byte
	hdr     int
}

// mqttCollectedDelivery is one fresh delivery the callback has allocated and collected for a
// batched admission, with what its send section needs. Its message expiry is judged at the flush,
// once, against the ORIGINAL properties.
type mqttCollectedDelivery struct {
	adm    *mqttBatchAdmission
	sub    *subscription
	pc, cc *client
	// lic is the deciding connection's licence, taken when the delivery was collected: an expiry
	// drop at the flush executes that connection's decision.
	lic         mqttDestroyLicence
	reply       string
	expirable   bool
	retainAsPub bool
	subID       int
	topic       []byte
	msg         []byte
	props       []byte
}

// mqttDeliveryCollector carries one run's collected deliveries until they are admitted and sent.
// It belongs to the subscription's dispatch goroutine for the length of one callback.
type mqttDeliveryCollector struct {
	sess  *mqttSession
	items []*mqttCollectedDelivery
	// reached counts the offers whose processing passed the delivery-done seam's point, so the
	// callback fires the seam for each once it has flushed (mqttDeliverQoS12Offer).
	reached int
}

// mqttTestHeldOffersHook observes a subscription's held offers each time one is held. Nil in
// production.
var mqttTestHeldOffersHook = new(mqttKnob[func(sub *subscription, held int)])

// mqttTestDeliveryFlushHook runs at the start of every non-empty flush, before the collected
// deliveries' expiry is judged. Nil in production.
var mqttTestDeliveryFlushHook = new(mqttKnob[func(sess *mqttSession, n int)])

// mqttDeliverMsgCbQoS12 is the callback attached to a JS durable subscription for a MQTT QoS 1+
// sub.
//
// ONE OFFER AT A TIME, IN THE ORDER JETSTREAM OFFERS THEM, on the subscription's one dispatch
// goroutine: that is the carrier of a non-shared subscription's per-(Topic, QoS) order
// (MQTT-4.6.0-6), and it is unchanged. What changes is how many offers one callback processes. An
// offer of a non-shared subscription that has more offers queued behind it is HELD (nothing is
// read, allocated, acknowledged or recorded for it), up to the batch bound; the callback that finds
// nothing more queued, or the bound reached, processes every held offer and then its own, in
// arrival order, collecting the fresh deliveries among them, and admits and sends what it collected
// (mqttDeliverQoS12Offer). An offer is never held when nothing is queued behind it, so a delivery
// at rest is processed by the callback that received it, with no added delay.
//
// A held offer is an unacknowledged JetStream delivery exactly like those still queued in the
// connection layer: if the subscription stops (unsubscribe, a re-SUBSCRIBE replacing it, the
// connection closing), neither is processed and AckWait re-offers both.
func mqttDeliverMsgCbQoS12(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if mqttDeliveryHoldable(sub) && mqttMoreQueued(sub) && len(sub.held) < mqttAdmissionBatchMax-1 {
		// A RE-OFFER OF AN OFFER ALREADY HELD is dropped; the held one keeps its position and
		// acknowledges the same stream sequence.
		if !mqttHeldOfferListed(sub.held, reply) {
			sub.held = append(sub.held, mqttHeldOffer{subject: subject, reply: reply, rmsg: rmsg, hdr: pc.pa.hdr})
		}
		if h := mqttTestHeldOffersHook.Get(); h != nil {
			h(sub, len(sub.held))
		}
		return
	}
	held := sub.held
	sub.held = nil
	col := &mqttDeliveryCollector{sess: sub.client.mqtt.sess}
	own := pc.pa.hdr
	for _, h := range held {
		pc.pa.hdr = h.hdr
		mqttDeliverQoS12Offer(sub, pc, h.subject, h.reply, h.rmsg, col)
	}
	pc.pa.hdr = own
	if !mqttHeldOfferListed(held, reply) {
		mqttDeliverQoS12Offer(sub, pc, subject, reply, rmsg, col)
	}
	col.flush()
	if dh := mqttTestDeliveryDoneHookV.Load(); dh != nil {
		for i := 0; i < col.reached; i++ {
			(*dh)(sub)
		}
	}
}

// mqttDeliveryHoldable reports whether this subscription's offers may be held: a non-shared QoS 1/2
// subscription. A shared subscription never holds and never collects — its QoS 0 delivery and QoS 2
// move reach the client outside the collector's flush points.
func mqttDeliveryHoldable(sub *subscription) bool {
	cc := sub.client
	if cc == nil || cc.mqtt == nil || cc.mqtt.sess == nil {
		return false
	}
	sess := cc.mqtt.sess
	sess.mu.Lock()
	ok := sub.mqtt != nil && !sub.mqtt.shared
	sess.mu.Unlock()
	return ok
}

// mqttMoreQueued reports whether another offer is queued behind the one whose callback is running.
// The connection layer counts the running one until its callback returns, hence more than one.
func mqttMoreQueued(sub *subscription) bool {
	f, _ := sub.queued.Load().(func() int)
	return f != nil && f() > 1
}

// mqttHeldOfferListed reports whether an offer of reply's stream sequence is already held.
func mqttHeldOfferListed(held []mqttHeldOffer, reply string) bool {
	seq, _, _, _, _ := ackReplyInfo(reply)
	if seq == 0 {
		return false
	}
	for _, h := range held {
		if hs, _, _, _, _ := ackReplyInfo(h.reply); hs == seq {
			return true
		}
	}
	return false
}

// collect adds one fresh delivery, flushing when the batch is full.
func (col *mqttDeliveryCollector) collect(d *mqttCollectedDelivery) {
	col.items = append(col.items, d)
	if len(col.items) >= mqttAdmissionBatchMax {
		col.flush()
	}
}

// unlockAndFlush releases the session lock and flushes: the exit of the delivery's decision region
// for every offer that does not join the collector, so whatever it does next — an acknowledgement,
// a NAK, a synchronous JetStream request, a wait, a send — follows the deliveries collected ahead
// of it.
//
// Session lock held on entry; released on return.
func (col *mqttDeliveryCollector) unlockAndFlush(sess *mqttSession) {
	sess.mu.Unlock()
	col.flush()
}

// flush admits the collected deliveries in one batch and sends each admitted one, in order, each
// send section complete before the next begins.
//
// Session lock NOT held on entry.
func (col *mqttDeliveryCollector) flush() {
	if col == nil || len(col.items) == 0 {
		return
	}
	items := col.items
	col.items = nil
	sess := col.sess
	if h := mqttTestDeliveryFlushHook.Get(); h != nil {
		h(sess, len(items))
	}
	// MESSAGE EXPIRY, judged here and only here for a collected delivery: one reduction of the
	// ORIGINAL interval by the time the message waited in the server. Only a delivery proven never
	// transmitted may be dropped on expiry ([MQTT-4.4.0-1]).
	expired := make([]bool, len(items))
	for i, d := range items {
		if len(d.props) > 0 && d.expirable {
			_, _, _, ts, _ := ackReplyInfo(d.reply)
			props, gone := mqttForwardExpiry(d.props, ts)
			if gone {
				expired[i] = true
				continue
			}
			d.props = props
		}
	}
	// IN ORDER: an expired delivery's disposal (a durable cleanup and its source acknowledgement)
	// waits for the live deliveries collected ahead of it to be admitted and sent, exactly as a
	// later offer's effects do; the live ones after it form the next batch.
	var live []*mqttCollectedDelivery
	for i, d := range items {
		if !expired[i] {
			live = append(live, d)
			continue
		}
		col.admitAndSend(live)
		live = nil
		removed := sess.dropProvenUnsentPublishEntry(d.adm.pi, d.lic)
		if !sess.finalizeDrainSource(d.cc, removed) {
			_, _ = sess.submitSourceAck(d.reply, false, nil)
		}
	}
	col.admitAndSend(live)
}

// admitAndSend admits collected deliveries in one batch and sends each admitted one, in order,
// each send section complete before the next begins.
//
// Session lock NOT held on entry.
func (col *mqttDeliveryCollector) admitAndSend(live []*mqttCollectedDelivery) {
	if len(live) == 0 {
		return
	}
	sess := col.sess
	adms := make([]*mqttBatchAdmission, len(live))
	for i, d := range live {
		adms[i] = d.adm
	}
	sess.admitInflightPublishBatch(live[0].cc, adms)
	for _, d := range live {
		if !d.adm.stored {
			continue
		}
		if h := mqttTestDeliveryAdmittedPauseV.Load(); h != nil {
			(*h)(sess, d.adm.pi)
		}
		sess.sendAdmittedPublish(d.pc, d.cc, d.sub, d.adm.pi, d.adm.qos, d.adm.sseq, d.reply, d.retainAsPub,
			d.subID, d.topic, d.msg, d.props)
	}
}
