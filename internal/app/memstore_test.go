package app_test

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wallet"
)

// memState é o estado do banco em memória usado apenas nos testes unitários
// da camada de aplicação. A semântica transacional é emulada por cópia do
// estado no início e substituição no COMMIT, sob um mutex global.
type memState struct {
	wallets map[uuid.UUID]wallet.RehydrateParams
	txs     map[uuid.UUID]wager.Snapshot
	ledger  map[uuid.UUID][]wallet.LedgerEntry
	outbox  []event.Envelope
	inbox   map[string]app.InboxMessage
}

func (s *memState) clone() *memState {
	c := &memState{
		wallets: maps.Clone(s.wallets),
		txs:     maps.Clone(s.txs),
		ledger:  make(map[uuid.UUID][]wallet.LedgerEntry, len(s.ledger)),
		outbox:  slices.Clone(s.outbox),
		inbox:   maps.Clone(s.inbox),
	}
	for k, v := range s.ledger {
		c.ledger[k] = slices.Clone(v)
	}
	return c
}

type memUOW struct {
	mu    sync.Mutex
	state *memState
}

func newMemUOW() *memUOW {
	return &memUOW{state: &memState{
		wallets: map[uuid.UUID]wallet.RehydrateParams{},
		txs:     map[uuid.UUID]wager.Snapshot{},
		ledger:  map[uuid.UUID][]wallet.LedgerEntry{},
		inbox:   map[string]app.InboxMessage{},
	}}
}

func (u *memUOW) Do(ctx context.Context, fn func(ctx context.Context, st app.Store) error) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	work := u.state.clone()
	if err := fn(ctx, &memStore{s: work}); err != nil {
		return err
	}
	u.state = work
	return nil
}

func (u *memUOW) ReadSnapshot(ctx context.Context, fn func(ctx context.Context, st app.Store) error) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return fn(ctx, &memStore{s: u.state.clone()})
}

func (u *memUOW) snapshot() *memState {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.state.clone()
}

type memStore struct{ s *memState }

func (m *memStore) Wallets() app.WalletRepository           { return memWallets{m.s} }
func (m *memStore) Transactions() app.TransactionRepository { return memTxs{m.s} }
func (m *memStore) Ledger() app.LedgerRepository            { return memLedger{m.s} }
func (m *memStore) Outbox() app.OutboxWriter                { return memOutbox{m.s} }
func (m *memStore) Inbox() app.InboxRepository              { return memInbox{m.s} }

type memWallets struct{ s *memState }

func (r memWallets) Insert(_ context.Context, w *wallet.Wallet) error {
	for _, p := range r.s.wallets {
		if p.PlayerID == w.PlayerID() && p.Balance.Currency() == w.Currency() {
			return app.ErrWalletAlreadyExists
		}
	}
	r.s.wallets[w.ID()] = wallet.RehydrateParams{ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(), CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt()}
	return nil
}

func (r memWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	p, ok := r.s.wallets[id]
	if !ok {
		return nil, app.ErrWalletNotFound
	}
	return wallet.Rehydrate(p)
}

func (r memWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.Get(ctx, id)
}

func (r memWallets) UpdateBalance(_ context.Context, w *wallet.Wallet, expected int64) error {
	p, ok := r.s.wallets[w.ID()]
	if !ok || p.Version != expected {
		return app.ConcurrencyConflict("version_mismatch", nil)
	}
	if p.Balance.IsNegative() || w.Balance().IsNegative() {
		panic("saldo negativo")
	}
	p.Balance, p.Version, p.UpdatedAt = w.Balance(), w.Version(), w.UpdatedAt()
	r.s.wallets[w.ID()] = p
	return nil
}

type memTxs struct{ s *memState }

func (r memTxs) Insert(_ context.Context, s wager.Snapshot) error {
	for _, o := range r.s.txs {
		if s.Origin == wager.OriginExternal && o.Origin == wager.OriginExternal && o.ProviderID == s.ProviderID &&
			(o.IdempotencyKey == s.IdempotencyKey || o.ExternalTransactionID == s.ExternalTransactionID) {
			return app.ConcurrencyConflict("unique", nil)
		}
		if s.Kind == wager.KindOpening && o.Kind == wager.KindOpening && o.WalletID == s.WalletID {
			return app.ConcurrencyConflict("unique", nil)
		}
	}
	r.s.txs[s.ID] = s
	return nil
}

func (r memTxs) Update(_ context.Context, s wager.Snapshot, expected wager.Status) error {
	o, ok := r.s.txs[s.ID]
	if !ok || o.Status != expected {
		return app.ConcurrencyConflict("status_mismatch", nil)
	}
	r.s.txs[s.ID] = s
	return nil
}

func (r memTxs) Get(_ context.Context, id uuid.UUID) (*wager.Transaction, error) {
	s, ok := r.s.txs[id]
	if !ok {
		return nil, app.ErrNotFound
	}
	return wager.Rehydrate(s)
}

func (r memTxs) find(pred func(wager.Snapshot) bool) (*wager.Transaction, error) {
	for _, s := range r.s.txs {
		if pred(s) {
			return wager.Rehydrate(s)
		}
	}
	return nil, nil
}

func (r memTxs) FindIdempotencyCandidates(_ context.Context, provider, key, ext string) ([]*wager.Transaction, error) {
	var out []*wager.Transaction
	for _, s := range r.s.txs {
		if s.Origin == wager.OriginExternal && s.ProviderID == provider && (s.IdempotencyKey == key || s.ExternalTransactionID == ext) {
			tx, err := wager.Rehydrate(s)
			if err != nil {
				return nil, err
			}
			out = append(out, tx)
		}
	}
	return out, nil
}

func (r memTxs) FindByExternalID(_ context.Context, provider, ext string) (*wager.Transaction, error) {
	return r.find(func(s wager.Snapshot) bool { return s.ProviderID == provider && s.ExternalTransactionID == ext })
}

func (r memTxs) HasProcessedReversal(_ context.Context, ref uuid.UUID) (bool, error) {
	for _, s := range r.s.txs {
		if s.Kind.IsReversal() && s.Status == wager.StatusProcessed && s.ReferenceTransactionID != nil && *s.ReferenceTransactionID == ref {
			return true, nil
		}
	}
	return false, nil
}

func (r memTxs) ListDuePending(_ context.Context, now time.Time, limit int) ([]app.PendingRef, error) {
	var out []app.PendingRef
	for _, s := range r.s.txs {
		if (s.Status == wager.StatusPending || s.Status == wager.StatusPendingReference) && s.NextAttemptAt != nil && !s.NextAttemptAt.After(now) {
			out = append(out, app.PendingRef{ID: s.ID, WalletID: s.WalletID})
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r memTxs) LockDuePending(_ context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error) {
	s, ok := r.s.txs[id]
	if !ok || s.Status.IsTerminal() || s.NextAttemptAt == nil || s.NextAttemptAt.After(now) {
		return nil, nil
	}
	return wager.Rehydrate(s)
}

func (r memTxs) WakeDependents(_ context.Context, provider, ext string, now time.Time) (int64, error) {
	var n int64
	for id, s := range r.s.txs {
		if s.Status == wager.StatusPendingReference && s.ProviderID == provider && s.ReferenceExternalTransactionID == ext {
			t := now
			s.NextAttemptAt = &t
			r.s.txs[id] = s
			n++
		}
	}
	return n, nil
}

type memLedger struct{ s *memState }

func (r memLedger) Insert(_ context.Context, e wallet.LedgerEntry) error {
	for _, o := range r.s.ledger[e.WalletID()] {
		if o.TransactionID() == e.TransactionID() || o.WalletVersion() == e.WalletVersion() {
			return app.ConcurrencyConflict("unique", nil)
		}
	}
	r.s.ledger[e.WalletID()] = append(r.s.ledger[e.WalletID()], e)
	return nil
}

func (r memLedger) List(_ context.Context, walletID uuid.UUID, after int64, limit int) ([]wallet.LedgerEntry, error) {
	var out []wallet.LedgerEntry
	for _, e := range r.s.ledger[walletID] {
		if e.WalletVersion() > after {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b wallet.LedgerEntry) int { return int(a.WalletVersion() - b.WalletVersion()) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r memLedger) Totals(_ context.Context, walletID uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	for _, e := range r.s.ledger[walletID] {
		if e.Direction() == wallet.Credit {
			t.CreditsMinor += e.Amount().Minor()
		} else {
			t.DebitsMinor += e.Amount().Minor()
		}
		t.Entries++
	}
	return t, nil
}

type memOutbox struct{ s *memState }

func (r memOutbox) Append(_ context.Context, evs ...event.Envelope) error {
	r.s.outbox = append(r.s.outbox, evs...)
	return nil
}

type memInbox struct{ s *memState }

func (r memInbox) Get(_ context.Context, consumer, id string) (*app.InboxMessage, error) {
	m, ok := r.s.inbox[consumer+"/"+id]
	if !ok {
		return nil, nil
	}
	return &m, nil
}

func (r memInbox) Record(_ context.Context, m app.InboxMessage) error {
	k := m.Consumer + "/" + m.MessageID
	if _, ok := r.s.inbox[k]; ok {
		return app.ConcurrencyConflict("unique", nil)
	}
	r.s.inbox[k] = m
	return nil
}

// fakeClock é um relógio controlável.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func mustMoney(s string) money.Money {
	m, err := money.Parse(s, "BRL")
	if err != nil {
		panic(err)
	}
	return m
}
