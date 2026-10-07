package postgres

import (
	"context"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxRecord é um evento reivindicado para publicação.
type OutboxRecord struct {
	EventID       uuid.UUID
	EventType     string
	EventVersion  int
	PartitionKey  string
	CorrelationID string
	Payload       string
	OccurredAt    time.Time
	Attempts      int
}

// OutboxStats resume a fila de publicação.
type OutboxStats struct {
	Pending          int64
	OldestAgeSeconds float64
}

// OutboxStore é usado pelo publicador (fora das transações de negócio).
type OutboxStore struct{ pool *pgxpool.Pool }

// NewOutboxStore cria o store.
func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

// Claim reivindica até limit eventos pendentes com um lease temporário.
// FOR UPDATE SKIP LOCKED permite vários publicadores disputando a mesma
// outbox sem bloqueio mútuo; um lease expirado (publicador morto) torna o
// evento elegível novamente para outra instância.
func (s *OutboxStore) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxRecord, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE outbox_events o
		   SET locked_by = $1,
		       locked_until = now() + ($2::BIGINT * INTERVAL '1 millisecond'),
		       attempts = o.attempts + 1
		 WHERE o.event_id IN (
		        SELECT event_id FROM outbox_events
		         WHERE published_at IS NULL
		           AND next_attempt_at <= now()
		           AND (locked_until IS NULL OR locked_until < now())
		         ORDER BY occurred_at, event_id
		         LIMIT $3
		         FOR UPDATE SKIP LOCKED)
		RETURNING o.event_id, o.event_type, o.event_version, o.partition_key,
		          COALESCE(o.correlation_id, ''), o.payload::TEXT, o.occurred_at, o.attempts`,
		owner, lease.Milliseconds(), limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		if err := rows.Scan(&r.EventID, &r.EventType, &r.EventVersion, &r.PartitionKey,
			&r.CorrelationID, &r.Payload, &r.OccurredAt, &r.Attempts); err != nil {
			return nil, classify(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	// RETURNING não garante ordem: publica na ordem de ocorrência.
	slices.SortFunc(out, func(a, b OutboxRecord) int {
		if c := a.OccurredAt.Compare(b.OccurredAt); c != 0 {
			return c
		}
		return slices.Compare(a.EventID[:], b.EventID[:])
	})
	return out, nil
}

// MarkPublished confirma a publicação. Devolve false se o evento já constava
// como publicado (outra instância concluiu primeiro).
func (s *OutboxStore) MarkPublished(ctx context.Context, eventID uuid.UUID) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		   SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		 WHERE event_id = $1 AND published_at IS NULL`, eventID)
	if err != nil {
		return false, classify(err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkFailed libera o lease e agenda nova tentativa com backoff.
func (s *OutboxStore) MarkFailed(ctx context.Context, eventID uuid.UUID, owner string, next time.Time, cause string) error {
	if len(cause) > 500 {
		cause = cause[:500]
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		   SET next_attempt_at = $3, last_error = $4, locked_by = NULL, locked_until = NULL
		 WHERE event_id = $1 AND locked_by = $2 AND published_at IS NULL`, eventID, owner, next, cause)
	return classify(err)
}

// Release devolve leases ainda não publicados (encerramento gracioso).
func (s *OutboxStore) Release(ctx context.Context, owner string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events SET locked_by = NULL, locked_until = NULL
		 WHERE locked_by = $1 AND published_at IS NULL`, owner)
	return classify(err)
}

// Stats devolve a quantidade pendente e a idade do evento mais antigo.
func (s *OutboxStore) Stats(ctx context.Context) (OutboxStats, error) {
	var st OutboxStats
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(EXTRACT(EPOCH FROM now() - MIN(occurred_at)), 0)::FLOAT8
		  FROM outbox_events WHERE published_at IS NULL`).Scan(&st.Pending, &st.OldestAgeSeconds)
	return st, classify(err)
}
