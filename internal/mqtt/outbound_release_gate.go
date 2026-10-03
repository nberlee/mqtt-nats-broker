// Copyright 2026 The mqtt-nats-bridge Authors
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

package mqtt

import (
	"encoding/json"
	"fmt"
)

// Releasing session metadata before physical cleanup (F-105, row 8).
//
// # The coupling being broken
//
// finishAdoptedTeardown runs recordGenRetirement, then the shared closing (seal, release,
// gate), then deleteTeardownTargets, then releaseTeardown — the metadata release. So metadata
// release waits on physical deletion finishing, and a CONNECT behind it waits with it.
//
// The design's requirement is the opposite coupling: release depends on DURABLE FACTS about the
// old generation, and physical deletion becomes a duty like any other, authorized later by the
// resource CAS. Question 5 is emphatically NOT "stop waiting" — it is "wait for the right thing".
//
// # The four facts
//
//  1. COMMITTED CLOSURE. The closure record exists and the generation's ledger is SEALED at a
//     bound. This is what makes "no new duty can enter G" a fact rather than a race: without it
//     a delayed admission can add a duty after the metadata said the generation was finished.
//  2. THE DURABLE DISCARD POLICY, where applicable. For Clean Start or expiry, the record saying
//     the old generation's obligations are DISCARDED rather than served, so a later reader does
//     not resurrect them. Where no discard applies this is vacuous rather than skipped.
//  3. DURABLE RECOVERY OWNERSHIP: the generation is reachable in its directory shard with an
//     open cursor, OR a durable certificate says its exact sealed inventory has completed. The
//     surviving duties are not abandoned, they are ENROLLED, and some broker serving the account
//     will come back for them. Releasing metadata while a duty exists that nothing will ever
//     traverse is the real hazard here, and this is the condition that excludes it.
//  4. THE AFFINE TARGET IS ITSELF ENROLLED. It lives in a transient teardown object built
//     inside finishAdoptedTeardown, so nothing durable names it — a crash after metadata
//     release loses it outright. It must already belong to the inventory. (The shared group
//     durables are not this gate's: a generation's shared ACQUISITIONS are released by the
//     shared closing from the generation's own acquisition ledger, behind a gate of their
//     own, before this one is consulted. See shared_lifecycle_ops.go.)
//
// # Why an open cursor cannot be required on its own
//
// Recovery may FINISH and retire the root before the teardown gets here, and requiring an open
// cursor would then deadlock the release forever. So a durable completion certificate satisfies
// condition 3 as well — and, because it must remain acceptable after the root and the entries
// are gone, it certifies the exact epoch, client, generation and sealed bound rather than
// pointing at records that no longer exist.
//
// Metadata release enables acceptance into a FRESH generation. It must never enable new
// obligations in the completed old one.
//
// # And it is VACUOUS for a generation that admitted nothing
//
// A generation with no ledger and no closure record never admitted an obligation through the
// authority, so there is no inventory for these facts to be about and nothing for a teardown to
// strand. That is what lets this gate land on the production path before the routes that create
// inventory are converted — the same shape checkDestructionAuthorized has, and for the same
// reason.

// mqttReleaseBlock names one fact that is missing.
type mqttReleaseBlock struct {
	Fact string
	Why  string
}

func (b mqttReleaseBlock) Error() string { return b.Fact + ": " + b.Why }

// mqttGenCompletionCertificate is the durable proof that one generation's whole sealed inventory
// completed.
//
// It exists so condition 3 can be satisfied AFTER the recovery root has been retired. That means
// it must stand on its own: naming the records it is about is useless once they are gone, so it
// carries the identity and the bound instead.
type mqttGenCompletionCertificate struct {
	ID    string `json:"id"`
	Epoch string `json:"epoch"`
	Gen   string `json:"gen"`
	// SealedHi is the inventory bound the sweep covered. Recorded because "completed" as a bare
	// flag is the inference from absence in a new hat: a certificate that did not say WHAT it
	// covered could be believed about a generation that later sealed at a higher bound.
	SealedHi uint64 `json:"sealed_hi"`
}

var mqttGenCertMembers = map[string]struct{}{
	"id": {}, "epoch": {}, "gen": {}, "sealed_hi": {},
}

// mqttGenCertSubject returns one generation's completion certificate subject.
func mqttGenCertSubject(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttGenCertClassTk + "." + idHash + "." + gen
}

// mqttGenCertClassTk is the completion certificate's record class.
const mqttGenCertClassTk = "done"

func mqttValidateGenCert(c *mqttGenCompletionCertificate, wantID, wantEpoch, wantGen string) error {
	if c == nil {
		return fmt.Errorf("completion certificate is absent")
	}
	if c.ID != wantID || c.Epoch != wantEpoch || c.Gen != wantGen {
		return fmt.Errorf("completion certificate names %q/%q/%q, not %q/%q/%q",
			c.ID, c.Epoch, c.Gen, wantID, wantEpoch, wantGen)
	}
	// A ZERO BOUND IS LEGAL, and it has to be: a generation that admitted nothing seals its
	// ledger at 0, so its completion covers 0. What the old rule protected — a certificate that
	// does not say what it covered — is preserved in two other places instead, and more exactly:
	// `sealed_hi` is a REQUIRED, non-null member of the body, so an omitted or null bound is
	// refused rather than decoding as this legal zero; and the gate compares the value against
	// the SEALED bound of the ledger that stands, which nothing deletes.
	return nil
}

// mqttAdoptedReleaseFacts is what a caller has established about the old generation.
//
// Passed in rather than read here, because the three reads have different owners and different
// failure dispositions — and a gate that did its own reading would decide, on a failure, whether
// to refuse or proceed, which is the caller's call and not this function's.
type mqttAdoptedReleaseFacts struct {
	// Closed is the generation's permanent closure record, nil when none stands.
	Closed *mqttGenClosure
	// Ledger is its inventory ledger, nil when the generation allocated nothing.
	Ledger *mqttGenLedger
	// RootEnrolled reports that this generation is REACHABLE BY A RECOVERY PASS: a root stands
	// at the address its closure records, inside its shard's high-water, naming this client,
	// this account recovery epoch and this generation.
	//
	// THE CURSOR IS NOT CONSULTED, and that correction matters the moment a driver exists. A
	// root whose sweep has finished but whose generation has not retired — unresolved work, or a
	// bound below the sealed high-water — is enrolled and WILL be swept again when the sweep
	// rolls. Asking whether the sweep is UNFINISHED would make one goroutine's liveness into the
	// gate's safety condition, and the last batch of every sweep persists exactly that state.
	//
	// What is still validated is the root's SHAPE and IDENTITY: only the liveness predicate is
	// gone, not the checks that say this record is this generation's enrolment.
	RootEnrolled bool
	// Cert is the durable completion certificate, nil when none stands.
	Cert *mqttGenCompletionCertificate
	// Discard says whether this generation's closure carried an authorized session discard,
	// which is what distinguishes "this generation ended" from "its obligations were authorized
	// to be abandoned". A later reader that gets this wrong SERVES obligations it should have
	// dropped, or drops obligations it should have served.
	//
	// A TRI-STATE, not a bool, and the zero value is UNKNOWN. A review required this
	// distinction explicitly — "distinguish KNOWN-INAPPLICABLE from UNKNOWN, with unknown
	// WITHHOLDING release" — because a caller that cannot answer otherwise passes false and the
	// gate reads it as "does not apply".
	//
	// THERE IS NOTHING FOR A CALLER TO GET WRONG, because the gatherer DERIVES it from the
	// closure record, which carries the disposition as a mandatory member and is written in the
	// transition that closes the generation — at the moment the protocol event is true, rather
	// than re-derived later by a teardown that no longer has it. That is also why there is no
	// second "and its record stands" fact beside this one: the record IS the disposition, so a
	// rule asking whether it is durable would be asking about the message it just read.
	Discard mqttFactState
}

// mqttFactState is what a caller has established about one condition.
//
// The zero value is UNKNOWN and every rule that consults one treats UNKNOWN as a refusal. That
// is the whole design: a caller that forgets a field gets a stall, not a release.
type mqttFactState uint8

const (
	// mqttFactUnknown: nobody established this either way. Blocks.
	mqttFactUnknown mqttFactState = iota
	// mqttFactApplies: the condition holds and its own evidence is required.
	mqttFactApplies
	// mqttFactDoesNotApply: POSITIVELY established not to hold. Never inferred from an absence.
	mqttFactDoesNotApply
)

// mqttAdoptedReleaseBlockedBy reports which facts forbid releasing this generation's metadata.
//
// Returns nil when release is permitted, which includes the VACUOUS case: a generation with no
// ledger and no closure admitted nothing through the authority, so there is no inventory for
// these facts to be about.
func mqttAdoptedReleaseBlockedBy(id, gen string, f mqttAdoptedReleaseFacts) []mqttReleaseBlock {
	if f.Ledger == nil && f.Closed == nil {
		// NOTHING WAS ADMITTED HERE. Not an exemption: there is no inventory, so there is no
		// duty a release could strand and no fact to establish about one.
		return nil
	}

	var blocks []mqttReleaseBlock
	if f.Closed == nil {
		blocks = append(blocks, mqttReleaseBlock{
			Fact: "committed closure",
			Why: fmt.Sprintf("generation %q of client %q has an inventory but no closure record, "+
				"so a delayed admission can still add a duty to it", gen, id),
		})
	}
	if f.Ledger == nil {
		blocks = append(blocks, mqttReleaseBlock{
			Fact: "a sealed inventory",
			Why: fmt.Sprintf("generation %q of client %q is closed but carries no ledger, so "+
				"this broker cannot say what its inventory was", gen, id),
		})
	} else if f.Ledger.State != mqttGenLedgerSealed {
		blocks = append(blocks, mqttReleaseBlock{
			Fact: "a sealed inventory",
			Why: fmt.Sprintf("generation %q of client %q stands OPEN at high-water %d: until the "+
				"shape of its inventory is fixed there is no bound for anything to have covered",
				gen, id, f.Ledger.Hi),
		})
	}

	if f.Discard == mqttFactUnknown {
		blocks = append(blocks, mqttReleaseBlock{
			Fact: "the discard disposition",
			Why: fmt.Sprintf("nothing has established whether this teardown discards generation "+
				"%q of client %q, and an unknown disposition is not a disposition: releasing on "+
				"one treats 'nobody said' as 'no'", gen, id),
		})
	}

	// EITHER a reachable enrolled root OR a certificate. Requiring the root alone deadlocks when
	// recovery finishes first; accepting the certificate alone would release a generation whose
	// duties nothing has ever traversed.
	//
	// A ROOT IS OWNERSHIP ONLY WHERE AN IMPLEMENTED EXECUTOR OWNS WHAT IT LEADS TO (F-105 7d). Its
	// executor performs a DISCARDED generation's retirement in full — every obligation ended and
	// its records deleted, every consumer disposed of, authorized and deleted, the affine and
	// retained work as before — so there a reachable root is durable recovery ownership even while
	// a supported step is withheld. A generation closed PRESERVING its obligations is not one any
	// executor continues (delivery, release and drain need a serving authority), so where its
	// sealed inventory holds anything a root alone would be a pointer at work no broker performs:
	// it must have certified complete. An EMPTY sealed inventory has no surviving work for anyone
	// to own, so its root suffices — which is the abandoned reservation, the one preserving
	// closure production writes. (An UNKNOWN disposition is refused by its own rule above and is
	// not restated here.)
	preservedWork := f.Discard == mqttFactDoesNotApply && f.Ledger != nil && f.Ledger.SealedHi != 0
	if !f.RootEnrolled || preservedWork {
		switch {
		case f.Cert == nil && f.RootEnrolled:
			blocks = append(blocks, mqttReleaseBlock{
				Fact: "durable recovery ownership",
				Why: fmt.Sprintf("generation %q of client %q was closed preserving its obligations "+
					"and has not certified complete: its recovery root leads to work no executor "+
					"performs, so it owns nothing", gen, id),
			})
		case f.Cert == nil:
			blocks = append(blocks, mqttReleaseBlock{
				Fact: "durable recovery ownership",
				Why: fmt.Sprintf("generation %q of client %q has neither a reachable recovery "+
					"root nor a completion certificate, so its surviving duties are abandoned "+
					"rather than enrolled and no broker will come back for them", gen, id),
			})
		case f.Ledger == nil:
			// A CERTIFICATE ABOUT A GENERATION WITH NO LEDGER is unattributable: there is no
			// sealed bound for it to have covered, so nothing can say whether it is about this
			// inventory or another.
			blocks = append(blocks, mqttReleaseBlock{
				Fact: "durable recovery ownership",
				Why: fmt.Sprintf("generation %q of client %q carries a completion certificate and "+
					"no ledger, so there is no sealed bound for that certificate to have covered",
					gen, id),
			})
		case f.Ledger.State != mqttGenLedgerSealed:
			blocks = append(blocks, mqttReleaseBlock{
				Fact: "durable recovery ownership",
				Why: fmt.Sprintf("generation %q of client %q carries a completion certificate "+
					"while its ledger stands OPEN: a sweep cannot have covered an inventory whose "+
					"shape is not yet fixed", gen, id),
			})
		case f.Cert.SealedHi != f.Ledger.SealedHi:
			// A CERTIFICATE ABOUT A DIFFERENT BOUND covers a different inventory. Accepting it
			// would release a generation on the strength of a sweep that finished before the
			// duties now standing were ever allocated.
			blocks = append(blocks, mqttReleaseBlock{
				Fact: "durable recovery ownership",
				Why: fmt.Sprintf("generation %q of client %q is sealed at %d but its completion "+
					"certificate covers %d, so the duties between them were never swept",
					gen, id, f.Ledger.SealedHi, f.Cert.SealedHi),
			})
		}
	}

	return blocks
}

// mqttAdoptedReleaseAuthorized is the single-error form, for callers that only need the verdict.
func mqttAdoptedReleaseAuthorized(id, gen string, f mqttAdoptedReleaseFacts) error {
	if blocks := mqttAdoptedReleaseBlockedBy(id, gen, f); len(blocks) > 0 {
		return fmt.Errorf("releasing the session metadata of generation %q is not authorized: %v",
			gen, blocks)
	}
	return nil
}

// gatherAdoptedReleaseFacts reads what the gate needs about one generation.
//
// Every read is kept apart from the verdict: this establishes facts, mqttAdoptedReleaseBlockedBy
// decides on them. A gate that read for itself would have to choose, on a failed read, between
// refusing and proceeding — and that choice belongs to the caller, which is the only thing that
// knows whether it is tearing down under a deadline or sweeping in the background.
//
// AN UNRESOLVED READ IS AN ERROR, never an absence. A closure or a ledger this broker cannot
// read may be the one saying the generation has an inventory, and treating it as missing would
// take the gate's vacuous path — releasing exactly the generation it exists to protect.
//
// The shared group durables are NOT among this gate's facts. Their release is the closing's own
// gate (sharedClosingComplete), asked on both teardown routes before the record goes and
// independently of this one's vacuous arm — which in production is always the arm taken, since
// no route admits an obligation through the authority yet.
func (as *mqttAccountSessionManager) gatherAdoptedReleaseFacts(id, epoch, gen string) (mqttAdoptedReleaseFacts, error) {

	var f mqttAdoptedReleaseFacts

	closed, _, status := as.jsa.loadGenClosure(id, epoch, gen)
	switch status {
	case mqttOutboundValid:
		f.Closed = closed
		// THE DISPOSITION IS READ, NOT ASKED FOR. It is a mandatory member of the closure
		// record — a body without it is refused rather than defaulted — and the closure is
		// written by the transition that closes the generation, which is the only act that has
		// the protocol event in front of it. A teardown re-deriving it later would be stating a
		// claim about something it can no longer observe.
		f.Discard = mqttFactDoesNotApply
		if closed.Discard {
			f.Discard = mqttFactApplies
		}
	case mqttOutboundAbsent:
		// AND AN ABSENT CLOSURE LEAVES IT UNKNOWN, which is the honest answer and not a
		// permissive one: there is no record, so nobody has said. The gate blocks on it, beside
		// blocking on the missing closure itself.
	default:
		return f, fmt.Errorf("the closure record of generation %q could not be resolved (status "+
			"%v), so this broker cannot say whether it has an inventory", gen, status)
	}

	led, _, lerr := as.jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return f, fmt.Errorf("the inventory ledger of generation %q could not be read: %w", gen, lerr)
	}
	f.Ledger = led

	// THE ROOT IS LOADED AT THE ADDRESS THE CLOSURE RECORDS, which is why it is read after the
	// closure rather than beside it. A generation with no closure has no address to read, and
	// the gate blocks on the missing closure itself rather than on a root it could not find.
	if f.Closed != nil {
		reach, rerr := as.recoveryRootReachable(id, epoch, gen, f.Closed)
		if rerr != nil {
			return f, rerr
		}
		f.RootEnrolled = reach
	}

	cert, cerr := as.jsa.loadGenCompletionCert(id, epoch, gen)
	if cerr != nil {
		return f, cerr
	}
	f.Cert = cert

	return f, nil
}

// recoveryRootReachable reports whether this generation is REACHABLE BY A RECOVERY PASS.
//
// "Reachable" is the whole question and the first version of this did not ask it. Finding a root
// record by wildcard proves only that a record EXISTS at some subject; the designed traversal
// starts from the shard HEADER and walks ordinals within its high-water, so a root outside that
// range — or in a shard with no header at all — is a record no pass will ever visit. Accepting
// one as recovery ownership let the gate clear a generation whose duties nothing would resume.
//
// THE ADDRESS COMES FROM THE CLOSURE, which the same batch wrote. The wildcard enumeration this
// replaces could return a SHORT answer — `streamSubjects` silently caps its reply — and a root it
// missed read as "no reachable root", which withholds a release that should have been permitted
// and, worse, would have made the outcome depend on how many subjects the filter happened to
// match. An exact address cannot be short.
//
// THE CURSOR IS NOT READ HERE AT ALL. A finished sweep is not a retirement: a root with
// unresolved work, or with a bound below the sealed high-water, is enrolled and will be swept
// again. What is still validated is that this record IS this generation's enrolment.
func (as *mqttAccountSessionManager) recoveryRootReachable(id, epoch, gen string,
	closed *mqttGenClosure) (bool, error) {

	if closed == nil {
		return false, nil
	}
	shard := closed.RootShard
	// THE CLOSURE'S OWN ADDRESS IS RE-DERIVED, not trusted: a record naming a shard this client
	// does not hash to addresses a root no traversal of this client's work reaches.
	if want := mqttDirShardFor(getHash(id)); shard != want {
		return false, fmt.Errorf("the closure of generation %q names recovery shard %d, but "+
			"client %q hashes to %d", gen, shard, id, want)
	}

	hdr, herr := as.jsa.loadDirShard(shard)
	if herr != nil {
		return false, herr
	}
	if hdr == nil {
		// NO HEADER, NO TRAVERSAL. Whatever root records exist in this shard, nothing starts a
		// pass over them — so no generation here is enrolled, however many roots are lying about.
		return false, nil
	}
	// OUTSIDE THE HIGH-WATER IS OUTSIDE EVERY TRAVERSAL. A root at an ordinal the header never
	// allocated was not enrolled by the batch that allocates them.
	if closed.RootOrd > hdr.Hi {
		return false, nil
	}

	r, _, rerr := as.jsa.loadRecoveryRoot(shard, closed.RootOrd)
	if rerr != nil {
		// UNREADABLE IS NOT ABSENT. A root this broker cannot interpret may be the one enrolling
		// exactly this generation.
		return false, rerr
	}
	if r == nil {
		// RETIRED, or never written. Either way nothing traverses this generation now, and the
		// gate falls through to the completion certificate — which is the evidence retirement
		// leaves behind precisely so this case does not deadlock.
		return false, nil
	}
	// THE EPOCH AND THE GENERATION ARE MATCHED TOO, as every other fact here is. A root stamped
	// with another account recovery epoch names a generation of a different incarnation of this
	// account, and treating it as this one's enrolment would clear a teardown on somebody else's
	// evidence.
	if r.ID != id || r.Epoch != epoch || r.Gen != gen {
		return false, nil
	}
	return true, nil
}

// loadDirShard reads one shard header, returning nil when none stands.
//
// ABSENT is a positive answer: a shard with no header has no traversal position, so nothing
// starts a pass over it. UNREADABLE is a refusal, because a header this broker cannot interpret
// may be the one whose high-water covers the root a teardown is asking about.
func (jsa *mqttJSA) loadDirShard(shard uint64) (*mqttDirShard, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttShardSubject(shard))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, fmt.Errorf("reading directory shard %d: %w", shard, err)
		}
		return nil, nil
	}
	if smsg == nil {
		return nil, nil
	}
	h, derr := mqttDecodeDirShard(smsg.Data, shard)
	if derr != nil {
		return nil, fmt.Errorf("directory shard %d cannot be interpreted: %w", shard, derr)
	}
	return h, nil
}

// loadGenCompletionCert reads one generation's completion certificate, nil when none stands.
func (jsa *mqttJSA) loadGenCompletionCert(id, epoch, gen string) (*mqttGenCompletionCertificate, error) {
	subj := mqttGenCertSubject(getHash(id), gen)
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, fmt.Errorf("reading the completion certificate of generation %q: %w", gen, err)
		}
		return nil, nil
	}
	if smsg == nil {
		return nil, nil
	}
	if err := mqttJSONMembersAreExactAndUnique(smsg.Data, mqttGenCertMembers,
		"a completion certificate"); err != nil {
		return nil, fmt.Errorf("the completion certificate of generation %q is unsupported: %w", gen, err)
	}
	// EVERY MEMBER IS REQUIRED, and `sealed_hi` is why. Exactness refuses unknown and duplicate
	// members; it does not require a member to be there, so an omitted bound would decode as zero
	// — which is now a LEGAL value, meaning "this generation admitted nothing". A certificate that
	// never stated its coverage must not be readable as one that covered an empty inventory.
	//
	// ONE RULE, not two: mqttJSONMembersArePresent already refuses a NULL member — "null is
	// absence spelled differently" — so a second not-null pass over the same set would be a
	// second statement of one fact, and a mutation of it survives precisely because the first
	// rule already catches everything it would.
	if err := mqttJSONMembersArePresent(smsg.Data, mqttGenCertMembers,
		"a completion certificate"); err != nil {
		return nil, fmt.Errorf("the completion certificate of generation %q is unsupported: %w", gen, err)
	}
	c := &mqttGenCompletionCertificate{}
	if err := json.Unmarshal(smsg.Data, c); err != nil {
		return nil, err
	}
	if err := mqttValidateGenCert(c, id, epoch, gen); err != nil {
		return nil, fmt.Errorf("the completion certificate of generation %q is unsupported: %w", gen, err)
	}
	return c, nil
}
