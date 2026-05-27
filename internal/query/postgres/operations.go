// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"blackcat.ca/gmeow/internal/contracts"
)

const operationColumns = `
	operation_id,
	request_hash,
	name,
	request_json,
	status,
	progress_json,
	result_json,
	error_text,
	created_at,
	updated_at,
	completed_at`

const operationTable = "public.interface_operations"

type operationScanner interface {
	Scan(dest ...any) error
}

func (index *Index) CreateOrGet(
	ctx context.Context,
	request contracts.CreateOperationRequest,
) (contracts.OperationRecord, bool, error) {
	record, found, err := index.runningOperationByRequestHash(ctx, request.RequestHash)
	if err != nil {
		return contracts.OperationRecord{}, false, err
	}
	if found {
		return record, false, nil
	}

	row := index.pool.QueryRow(ctx, `
		INSERT INTO `+operationTable+` (
			operation_id, request_hash, name, request_json, status
		)
		VALUES ($1, $2, $3, $4::jsonb, $5)
		RETURNING `+operationColumns,
		request.OperationID,
		request.RequestHash,
		request.Name,
		string(request.Request),
		string(contracts.OperationStatusRunning),
	)

	record, err = scanOperation(row)
	if err == nil {
		return record, true, nil
	}
	if uniqueViolation(err) {
		record, found, err = index.runningOperationByRequestHash(ctx, request.RequestHash)
		if err != nil {
			return contracts.OperationRecord{}, false, err
		}
		if !found {
			return contracts.OperationRecord{}, false,
				fmt.Errorf(
					"running operation request hash %s not found after conflict",
					request.RequestHash,
				)
		}

		return record, false, nil
	}

	return contracts.OperationRecord{}, false, err
}

func (index *Index) runningOperationByRequestHash(
	ctx context.Context,
	requestHash string,
) (contracts.OperationRecord, bool, error) {
	row := index.pool.QueryRow(ctx, `
		SELECT `+operationColumns+`
		FROM `+operationTable+`
		WHERE request_hash = $1
		  AND status = $2
		ORDER BY created_at
		LIMIT 1`,
		requestHash,
		string(contracts.OperationStatusRunning),
	)

	return scanOptionalOperation(row)
}

func uniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	return pgErr.Code == "23505"
}

func (index *Index) AppendProgress(
	ctx context.Context,
	operationID string,
	event contracts.OperationProgressEvent,
) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}

	tag, err := index.pool.Exec(ctx, `
		UPDATE `+operationTable+`
		SET progress_json = progress_json || jsonb_build_array($2::jsonb),
		    updated_at = now()
		WHERE operation_id = $1`,
		operationID,
		string(encoded),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("operation %s not found", operationID)
	}

	return nil
}

func (index *Index) Complete(
	ctx context.Context,
	operationID string,
	result json.RawMessage,
) error {
	tag, err := index.pool.Exec(ctx, `
		UPDATE `+operationTable+`
		SET status = $2,
		    result_json = $3::jsonb,
		    error_text = '',
		    updated_at = now(),
		    completed_at = now()
		WHERE operation_id = $1`,
		operationID,
		string(contracts.OperationStatusComplete),
		string(result),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("operation %s not found", operationID)
	}

	return nil
}

func (index *Index) Fail(ctx context.Context, operationID, message string) error {
	tag, err := index.pool.Exec(ctx, `
		UPDATE `+operationTable+`
		SET status = $2,
		    error_text = $3,
		    updated_at = now(),
		    completed_at = now()
		WHERE operation_id = $1`,
		operationID,
		string(contracts.OperationStatusFailed),
		message,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("operation %s not found", operationID)
	}

	return nil
}

func (index *Index) Get(
	ctx context.Context,
	operationID string,
) (contracts.OperationRecord, bool, error) {
	row := index.pool.QueryRow(ctx, `
		SELECT `+operationColumns+`
		FROM `+operationTable+`
		WHERE operation_id = $1`,
		operationID,
	)

	return scanOptionalOperation(row)
}

func (index *Index) GetByRequestHash(
	ctx context.Context,
	requestHash string,
) (contracts.OperationRecord, bool, error) {
	row := index.pool.QueryRow(ctx, `
		SELECT `+operationColumns+`
		FROM `+operationTable+`
		WHERE request_hash = $1`,
		requestHash,
	)

	return scanOptionalOperation(row)
}

func scanOptionalOperation(
	row operationScanner,
) (contracts.OperationRecord, bool, error) {
	record, err := scanOperation(row)
	if err == nil {
		return record, true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.OperationRecord{}, false, nil
	}

	return contracts.OperationRecord{}, false, err
}

func scanOperation(row operationScanner) (contracts.OperationRecord, error) {
	var (
		record       contracts.OperationRecord
		status       string
		requestJSON  []byte
		progressJSON []byte
		resultJSON   []byte
		completedAt  *time.Time
	)

	if err := row.Scan(
		&record.OperationID,
		&record.RequestHash,
		&record.Name,
		&requestJSON,
		&status,
		&progressJSON,
		&resultJSON,
		&record.Error,
		&record.CreatedAt,
		&record.UpdatedAt,
		&completedAt,
	); err != nil {
		return contracts.OperationRecord{}, err
	}

	record.Status = contracts.OperationStatus(status)
	record.Request = append(json.RawMessage{}, requestJSON...)
	record.Result = append(json.RawMessage{}, resultJSON...)
	if completedAt != nil {
		record.CompletedAt = completedAt.UTC()
	}
	if len(progressJSON) > 0 {
		if err := json.Unmarshal(progressJSON, &record.Progress); err != nil {
			return contracts.OperationRecord{}, err
		}
	}

	return record, nil
}
