package mqtt

// testAcquirePubRel drives the ready-slot PUBREL acquisition the way admission does, supplying the
// lineage from the register as it stands (F-105 row 2 threads it from admission's fresh read).
func (sess *mqttSession) testAcquirePubRel(c *client, ref mqttInflightSlotRef) *mqttSendAction {
	lin := _EMPTY_
	if sess.asm != nil {
		if cur, _, err := sess.asm.loadInflightRecord(ref.name.subject); err == nil && cur != nil {
			lin = cur.Lin
		}
	}
	act, _ := sess.mqttAcquirePubRelAction(c, ref, lin)
	return act
}
