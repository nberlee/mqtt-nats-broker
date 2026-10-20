package mqtt

import (
	"fmt"
	"sort"
	"testing"
)

// Coverage for how consumerList pages CONSUMER.LIST. The walk advances by the positions a
// page COVERED, as the pinned handlers define them: the standalone handler examines
// positions until it has returned 256 infos (an offline consumer is listed in Missing and
// skipped), the clustered handler selects 256 assignments and reports each as answered, as
// a synthetic offline entry that is also in Missing, or as timed out and in Missing only.
// The fake pages below are shaped after those two handlers, because a standalone test
// server cannot produce a Missing entry at all.

// testListPages is a fake CONSUMER.LIST: one prepared page per expected offset. An offset
// nobody prepared fails the test at once, so a wrong advance is reported where it happens
// rather than as a hang or a silent short walk.
type testListPages struct {
	t         *testing.T
	pages     map[int]*JSApiConsumerListResponse
	requested []int
}

func (p *testListPages) fetch(offset int) (*JSApiConsumerListResponse, error) {
	p.t.Helper()
	p.requested = append(p.requested, offset)
	page, ok := p.pages[offset]
	if !ok {
		p.t.Fatalf("the walk requested offset %d, which no page was prepared for (requested so far %v)", offset, p.requested)
	}
	return page, nil
}

func testListInfos(prefix string, n int) []*ConsumerInfo {
	out := make([]*ConsumerInfo, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-%03d", prefix, i)
		out = append(out, &ConsumerInfo{Name: name, Config: &ConsumerConfig{Durable: name, Name: name}})
	}
	return out
}

func testListNames(n int, prefix string) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("%s-%03d", prefix, i))
	}
	return out
}

func testReturnedNames(infos []*ConsumerInfo) []string {
	out := make([]string, 0, len(infos))
	for _, ci := range infos {
		out = append(out, mqttConsumerListEntryName(ci))
	}
	return out
}

// testListPage builds one page as a handler would answer it at offset.
func testListPage(total, offset int, consumers []*ConsumerInfo, missing []string) *JSApiConsumerListResponse {
	r := &JSApiConsumerListResponse{Consumers: consumers, Missing: missing}
	r.Total, r.Offset, r.Limit = total, offset, 256
	return r
}

// A standalone-shaped page that examined offline consumers: it returned 256 infos AND
// listed 3 in Missing, so it covered 259 positions and the next page starts at 259.
func TestConsumerListAdvancesPastOfflineEntries(t *testing.T) {
	p := &testListPages{t: t, pages: map[int]*JSApiConsumerListResponse{
		0:   testListPage(300, 0, testListInfos("a", 256), testListNames(3, "off")),
		259: testListPage(300, 259, testListInfos("b", 41), nil),
	}}
	got, err := mqttWalkConsumerList(p.fetch)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(p.requested) != 2 || p.requested[1] != 259 {
		t.Fatalf("the walk requested %v; after a page covering 256 infos and 3 offline positions it must ask for 259", p.requested)
	}
	if len(got) != 256+41 {
		t.Fatalf("the walk returned %d consumers, want %d (offline ones are not returned)", len(got), 256+41)
	}
}

// A clustered page whose every selected assignment timed out returns no infos and 256
// Missing names. It is not the end: the walk must go on to the next page and return it.
func TestConsumerListContinuesPastAnEntirelyMissingPage(t *testing.T) {
	p := &testListPages{t: t, pages: map[int]*JSApiConsumerListResponse{
		0:   testListPage(600, 0, testListInfos("a", 256), nil),
		256: testListPage(600, 256, nil, testListNames(256, "slow")),
		512: testListPage(600, 512, testListInfos("c", 88), nil),
	}}
	got, err := mqttWalkConsumerList(p.fetch)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if want := []int{0, 256, 512}; fmt.Sprint(p.requested) != fmt.Sprint(want) {
		t.Fatalf("the walk requested %v, want %v: an entirely-missing page must not end it", p.requested, want)
	}
	if len(got) != 256+88 {
		t.Fatalf("the walk returned %d consumers, want %d", len(got), 256+88)
	}
}

// A clustered all-peers-offline consumer is reported twice — as a synthetic entry with no
// top-level name, and in Missing. It occupies ONE position, so it is counted once in the
// advance, and it is not returned, because the LIST could not report it. Nonterminal, so
// the next requested offset is what discriminates counting it once from counting it twice.
func TestConsumerListCountsASyntheticOfflineEntryOnce(t *testing.T) {
	synthetic := &ConsumerInfo{Config: &ConsumerConfig{Durable: "offline-x", Name: "offline-x"}}
	page0 := append([]*ConsumerInfo{synthetic}, testListInfos("a", 255)...)
	p := &testListPages{t: t, pages: map[int]*JSApiConsumerListResponse{
		0:   testListPage(300, 0, page0, []string{"offline-x"}),
		256: testListPage(300, 256, testListInfos("b", 44), nil),
	}}
	got, err := mqttWalkConsumerList(p.fetch)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(p.requested) != 2 || p.requested[1] != 256 {
		t.Fatalf("the walk requested %v; a synthetic offline entry that is also in Missing occupies one position, so the next offset is 256", p.requested)
	}
	for _, name := range testReturnedNames(got) {
		if name == "offline-x" || name == _EMPTY_ {
			t.Fatalf("the walk returned %q, a consumer the LIST could not report", name)
		}
	}
}

// An unnamed ephemeral's synthetic entry has no derivable name at all; Missing alone
// accounts for its position, and the entry is dropped.
func TestConsumerListDropsAnUnattributableEntry(t *testing.T) {
	unnamed := &ConsumerInfo{Config: &ConsumerConfig{}}
	page0 := append([]*ConsumerInfo{unnamed}, testListInfos("a", 255)...)
	p := &testListPages{t: t, pages: map[int]*JSApiConsumerListResponse{
		0:   testListPage(300, 0, page0, []string{"ephemeral-gen"}),
		256: testListPage(300, 256, testListInfos("b", 44), nil),
	}}
	got, err := mqttWalkConsumerList(p.fetch)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(p.requested) != 2 || p.requested[1] != 256 {
		t.Fatalf("the walk requested %v, want the second page at 256", p.requested)
	}
	if len(got) != 255+44 {
		t.Fatalf("the walk returned %d consumers, want %d", len(got), 255+44)
	}
}

// Pages are not a snapshot: an insert below the offset makes the next page repeat entries
// the previous one returned. They are returned once.
func TestConsumerListReturnsARepeatedConsumerOnce(t *testing.T) {
	first := testListInfos("a", 256)
	second := append(append([]*ConsumerInfo{}, first[254:]...), testListInfos("b", 40)...)
	p := &testListPages{t: t, pages: map[int]*JSApiConsumerListResponse{
		0:   testListPage(298, 0, first, nil),
		256: testListPage(298, 256, second, nil),
	}}
	got, err := mqttWalkConsumerList(p.fetch)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	names := testReturnedNames(got)
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			t.Fatalf("the walk returned %q twice", n)
		}
		seen[n] = true
	}
	if len(names) != 256+40 {
		t.Fatalf("the walk returned %d consumers, want %d", len(names), 256+40)
	}
}

// The clustered handler answers an exhausted offset with an empty page and Total 0.
func TestConsumerListEndsOnAnExhaustedOffset(t *testing.T) {
	p := &testListPages{t: t, pages: map[int]*JSApiConsumerListResponse{
		0: testListPage(0, 0, nil, nil),
	}}
	got, err := mqttWalkConsumerList(p.fetch)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(got) != 0 || len(p.requested) != 1 {
		t.Fatalf("an empty first page must end the walk with nothing, got %d consumer(s) after %v", len(got), p.requested)
	}
}

// Against a real server, across the LIST page boundary: every named durable comes back
// once, and each raw page carries the shape the walk relies on (a 256 limit, names
// ascending within the page).
func TestConsumerListWalksARealServerAcrossPages(t *testing.T) {
	e := testNewOrphanEnv(t, "consumer-list-paging")
	const n = 300
	created := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("paging-durable-%03d", i)
		// A pull durable: no delivery subject, so no MQTT sweep judges it.
		if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{Durable: name, AckPolicy: AckExplicit, DeliverPolicy: DeliverNew},
		}); err != nil {
			t.Fatalf("create %q: %v", name, err)
		}
		created = append(created, name)
	}

	got, err := e.asm.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("consumerList: %v", err)
	}
	names := map[string]int{}
	for _, ci := range got {
		names[mqttConsumerListEntryName(ci)]++
	}
	for _, name := range created {
		if names[name] != 1 {
			t.Fatalf("durable %q was returned %d time(s), want once", name, names[name])
		}
	}

	for offset := 0; ; {
		page, err := e.asm.jsa.consumerListPage(mqttStreamName, offset)
		if err != nil {
			t.Fatalf("page at %d: %v", offset, err)
		}
		if page.Limit != 256 {
			t.Fatalf("the page limit is %d, not the 256 the design cites", page.Limit)
		}
		pageNames := testReturnedNames(page.Consumers)
		if !sort.StringsAreSorted(pageNames) {
			t.Fatalf("the page at %d is not in name order: %v", offset, pageNames)
		}
		if len(page.Consumers) == 0 || offset+len(page.Consumers) >= page.Total {
			break
		}
		offset += len(page.Consumers)
	}
}
