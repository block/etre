// Copyright 2017-2020, Square, Inc.

package mock

import (
	"context"

	"github.com/square/etre"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
)

type EntityStore struct {
	ReadEntityFunc        func(ctx context.Context, entityType string, entityId string, f etre.QueryFilter) (etre.Entity, error)
	DeleteEntityLabelFunc func(context.Context, entity.WriteOp, string) (etre.Entity, error)
	CreateEntitiesFunc    func(context.Context, entity.WriteOp, []etre.Entity) ([]string, error)
	UpdateEntitiesFunc    func(context.Context, entity.WriteOp, query.Query, etre.Entity) ([]etre.Entity, error)
	DeleteEntitiesFunc    func(context.Context, entity.WriteOp, query.Query) ([]etre.Entity, error)
	DeleteLabelFunc       func(context.Context, entity.WriteOp, string) (etre.Entity, error)
	StreamEntitiesFunc    func(ctx context.Context, entityType string, q query.Query, f etre.QueryFilter) <-chan entity.EntityResult
}

func (s EntityStore) DeleteEntityLabel(ctx context.Context, wo entity.WriteOp, label string) (etre.Entity, error) {
	if s.DeleteEntityLabelFunc != nil {
		return s.DeleteEntityLabelFunc(ctx, wo, label)
	}
	return nil, nil
}

func (s EntityStore) CreateEntities(ctx context.Context, wo entity.WriteOp, entities []etre.Entity) ([]string, error) {
	if s.CreateEntitiesFunc != nil {
		return s.CreateEntitiesFunc(ctx, wo, entities)
	}
	return nil, nil
}

func (s EntityStore) ReadEntity(ctx context.Context, entityType string, entityId string, f etre.QueryFilter) (etre.Entity, error) {
	if s.ReadEntityFunc != nil {
		return s.ReadEntityFunc(ctx, entityType, entityId, f)
	}
	return nil, nil
}

func (s EntityStore) UpdateEntities(ctx context.Context, wo entity.WriteOp, q query.Query, u etre.Entity) ([]etre.Entity, error) {
	if s.UpdateEntitiesFunc != nil {
		return s.UpdateEntitiesFunc(ctx, wo, q, u)
	}
	return nil, nil
}

func (s EntityStore) DeleteEntities(ctx context.Context, wo entity.WriteOp, q query.Query) ([]etre.Entity, error) {
	if s.DeleteEntitiesFunc != nil {
		return s.DeleteEntitiesFunc(ctx, wo, q)
	}
	return nil, nil
}

func (s EntityStore) DeleteLabel(ctx context.Context, wo entity.WriteOp, label string) (etre.Entity, error) {
	if s.DeleteLabelFunc != nil {
		return s.DeleteLabelFunc(ctx, wo, label)
	}
	return etre.Entity{}, nil
}

func (s EntityStore) StreamEntities(ctx context.Context, entityType string, q query.Query, f etre.QueryFilter) <-chan entity.EntityResult {
	if s.StreamEntitiesFunc != nil {
		return s.StreamEntitiesFunc(ctx, entityType, q, f)
	}
	return DoStreamEntities(nil, nil)
}

func DoStreamEntities(entities []etre.Entity, err error) <-chan entity.EntityResult {
	ch := make(chan entity.EntityResult)
	go func() {
		defer close(ch)
		if err != nil {
			ch <- entity.EntityResult{Err: err}
			return
		}
		for _, e := range entities {
			ch <- entity.EntityResult{Entity: e}
		}
	}()
	return ch
}
