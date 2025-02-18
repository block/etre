// Copyright 2017-2020, Square, Inc.

package entity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/square/etre"
	"github.com/square/etre/entity"
	"github.com/square/etre/query"
	"github.com/square/etre/test"
	"github.com/square/etre/test/mock"
)

var (
	client    *mongo.Client
	coll      map[string]*mongo.Collection
	testNodes []etre.Entity

	username    = "test_user"
	entityType  = "nodes"
	entityTypes = []string{entityType}
)

var wo = entity.WriteOp{
	EntityType: entityType,
	Caller:     username,
}

func setup(t *testing.T, cdcm *mock.CDCStore) entity.Store {
	if coll == nil {
		var err error
		client, coll, err = test.DbCollections(entityTypes)
		require.NoError(t, err)
	}

	// Reset the collection: delete all entities and insert the standard test entities
	nodesColl := coll[entityType]
	_, err := nodesColl.DeleteMany(context.TODO(), bson.D{{}})
	require.NoError(t, err)

	// Create unique index on "x"
	iv := nodesColl.Indexes()
	if _, err := iv.DropAll(context.TODO()); err != nil {
		// Don't error; Mongo returns an error if the namespace doesn't exist,
		// which is what we want, so it's not an error in this context
		t.Log(err)
	}
	idx := mongo.IndexModel{
		Keys:    bson.D{{"x", 1}},
		Options: options.Index().SetUnique(true),
	}
	_, err = iv.CreateOne(context.TODO(), idx)
	require.NoError(t, err)

	testNodes = []etre.Entity{
		{"_type": entityType, "_rev": int64(0), "x": int64(2), "y": "a", "z": int64(9), "foo": ""},
		{"_type": entityType, "_rev": int64(0), "x": int64(4), "y": "b", "bar": ""},
		{"_type": entityType, "_rev": int64(0), "x": int64(6), "y": "b", "bar": ""},
	}
	res, err := nodesColl.InsertMany(context.TODO(), docs(testNodes))
	require.NoError(t, err)
	require.Len(t, res.InsertedIDs, len(testNodes), "mongo-driver returned %d doc ids, expected %d", len(res.InsertedIDs), len(testNodes))
	for i, id := range res.InsertedIDs {
		testNodes[i]["_id"] = id.(primitive.ObjectID)
	}

	return entity.NewStore(coll, cdcm)
}

func docs(entities []etre.Entity) []interface{} {
	docs := make([]interface{}, len(entities))
	for i, e := range entities {
		docs[i] = e
	}
	return docs
}

// --------------------------------------------------------------------------
// Read
// --------------------------------------------------------------------------

func TestReadEntitiesWithAllOperators(t *testing.T) {
	// Test all operators: in, notin, =, ==, !=, (has) y, (does not have) !foo, <, >
	// All values are set such that it only matches the first test node to make
	// testing easier and ensure we don't match the other entities.
	store := setup(t, &mock.CDCStore{})
	queries := []string{
		"y in (a, z)",
		"y notin (b, c)",
		"y = a",
		"y == a",
		"y != b",
		"foo",
		"!bar",
		"z > 1",
		"z < 10",
	}
	expect := []etre.Entity{testNodes[0]}
	for _, qs := range queries {
		q, err := query.Translate(qs)
		require.NoError(t, err)

		actual, err := store.ReadEntities(entityType, q, etre.QueryFilter{})
		require.NoError(t, err)
		assert.Equal(t, expect, actual)
	}
}

type readTest struct {
	query  string
	expect []etre.Entity
}

func TestReadEntitiesMatching(t *testing.T) {
	// Test various combinations of queries to ensure that we match and return
	// the correct entities. This is the fundamental job of Etre, so it should
	// be very thoroughly tested.
	store := setup(t, &mock.CDCStore{})
	readTests := []readTest{
		{
			// A more complex query (multiple operators) matching only 1st test node
			query:  "foo, !bar, z>1",
			expect: testNodes[:1],
		},
		{
			// All test nodes have label "y"
			query:  "y",
			expect: testNodes,
		},
		{
			// First test node has x=2, so this matches 2nd and 3rd test nodes
			query:  "x>2",
			expect: testNodes[1:],
		},
		{
			// All test nodes have label y but none with y=y, so none match
			query:  "y=y",
			expect: []etre.Entity{},
		},
	}
	for _, rt := range readTests {
		q, err := query.Translate(rt.query)
		require.NoError(t, err)

		got, err := store.ReadEntities(entityType, q, etre.QueryFilter{})
		require.NoError(t, err)
		assert.Equal(t, rt.expect, got)
	}
}

func TestReadEntitiesFilterDistinct(t *testing.T) {
	// Test that etre.QueryFilter{Distinct: true} returns a list of unique values
	// for one label. The 1st test node has y=a and the 2nd and 3rd both have y=b,
	// so the unique values are [a,b].
	store := setup(t, &mock.CDCStore{})
	q, err := query.Translate("y") // all test nodes have label "y"
	require.NoError(t, err)

	f := etre.QueryFilter{
		ReturnLabels: []string{"y"}, // only works with 1 return label
		Distinct:     true,
	}
	got, err := store.ReadEntities(entityType, q, f)
	require.NoError(t, err)

	expect := []etre.Entity{
		{"y": "a"}, // from 1st test node
		{"y": "b"}, // from 2nd and 3rd test nodes
	}
	assert.Equal(t, expect, got)
}

func TestReadEntitiesFilterReturnLabels(t *testing.T) {
	// Test that etre.QueryFilter{ReturnLabels: []string{x}} returns only that
	// label and not the others (y, z, bar, foo). We'll select/match by label y
	// but return only label x.
	store := setup(t, &mock.CDCStore{})
	q, err := query.Translate("y") // all test nodes have label "y"
	require.NoError(t, err)

	f := etre.QueryFilter{
		ReturnLabels: []string{"x"}, // testing this
	}
	expect := []etre.Entity{
		{"x": int64(2)},
		{"x": int64(4)},
		{"x": int64(6)},
	}
	got, err := store.ReadEntities(entityType, q, f)
	require.NoError(t, err)
	assert.Equal(t, expect, got)
}

func TestReadEntitiesFilterReturnMetalabels(t *testing.T) {
	store := setup(t, &mock.CDCStore{})
	q, err := query.Translate("y=a")
	require.NoError(t, err)

	actual, err := store.ReadEntities(entityType, q, etre.QueryFilter{ReturnLabels: []string{"_id", "_type", "_rev", "y"}})
	require.NoError(t, err)

	expect := []etre.Entity{
		{"_id": testNodes[0]["_id"], "_type": entityType, "_rev": int64(0), "y": "a"},
	}
	assert.Equal(t, expect, actual)
}

// --------------------------------------------------------------------------
// Create
// --------------------------------------------------------------------------

func TestCreateEntitiesMultiple(t *testing.T) {
	// Test basic insert of multiple new entities. Also test that CDCEvent is
	// logged with the correct info about the new entities.
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setup(t, cdcm)

	testData := []etre.Entity{
		etre.Entity{"x": 7},
		etre.Entity{"x": 8},
		etre.Entity{"x": 9, "_setId": "343", "_setOp": "something", "_setSize": 1},
	}
	ids, err := store.CreateEntities(wo, testData)
	require.NoError(t, err)
	assert.Len(t, ids, len(testData))

	// Verify that the last CDC event we create is as expected.
	id1, _ := primitive.ObjectIDFromHex(ids[0])
	id2, _ := primitive.ObjectIDFromHex(ids[1])
	id3, _ := primitive.ObjectIDFromHex(ids[2])
	expectEvents := []etre.CDCEvent{
		{
			Id:         gotEvents[0].Id, // non-deterministic
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(0),
			Ts:         gotEvents[0].Ts, // non-deterministic
			Caller:     username,
			Op:         "i",
			Old:        nil,
			New:        &etre.Entity{"_id": id1, "_type": entityType, "_rev": int64(0), "x": 7},
		},
		{
			Id:         gotEvents[1].Id, // non-deterministic
			EntityId:   id2.Hex(),
			EntityType: entityType,
			EntityRev:  int64(0),
			Ts:         gotEvents[1].Ts, // non-deterministic
			Caller:     username,
			Op:         "i",
			Old:        nil,
			New:        &etre.Entity{"_id": id2, "_type": entityType, "_rev": int64(0), "x": 8},
		},
		{
			Id:         gotEvents[2].Id, // non-deterministic
			EntityId:   id3.Hex(),
			EntityType: entityType,
			EntityRev:  int64(0),
			Ts:         gotEvents[2].Ts, // non-deterministic
			Caller:     username,
			Op:         "i",
			Old:        nil,
			New:        &etre.Entity{"_id": id3, "_type": entityType, "_rev": int64(0), "x": 9, "_setId": "343", "_setOp": "something", "_setSize": 1},
			SetId:      "343",
			SetOp:      "something",
			SetSize:    1,
		},
	}
	assert.Equal(t, expectEvents, gotEvents)
}

func TestCreateEntitiesMultiplePartialSuccess(t *testing.T) {
	// Test that create handles dupes and returns partial success. The first
	// entity here works, but the 2nd is a dupe of x=6 in the test nodes.
	// The 3rd isn't created because the first error stops the insert process.
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setup(t, cdcm)

	// Expect first two documents to be inserted and third to fail
	testData := []etre.Entity{
		etre.Entity{"x": 5}, // ok
		etre.Entity{"x": 6}, // dupe
		etre.Entity{"x": 7}, // would be ok but blocked by dupe
	}
	ids, err := store.CreateEntities(wo, testData)
	require.Error(t, err)
	dberr, ok := err.(entity.DbError)
	require.True(t, ok, "got error type %#v, expected entity.DbError", err)
	assert.Equal(t, "duplicate-entity", dberr.Type)
	assert.Len(t, ids, 1)

	// Only x=5 written/inserted, so only a CDC event for it
	id1, _ := primitive.ObjectIDFromHex(ids[0])
	expectEvents := []etre.CDCEvent{
		{
			Id:         gotEvents[0].Id, // non-deterministic
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(0),
			Ts:         gotEvents[0].Ts, // non-deterministic
			Caller:     username,
			Op:         "i",
			Old:        nil,
			New:        &etre.Entity{"_id": id1, "_type": entityType, "_rev": int64(0), "x": 5},
		},
	}
	assert.Equal(t, expectEvents, gotEvents)
}

// --------------------------------------------------------------------------
// Update
// --------------------------------------------------------------------------

func TestUpdateEntities(t *testing.T) {
	// Test that basic update changes the correct label values. We'll change
	// the test nodes, but setup() will restore them for other tests. Also
	// test the CDC events correctly reflect the changes.
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setup(t, cdcm)

	// ----------------------------------------------------------------------
	// This matches first test node
	q, err := query.Translate("y=a")
	require.NoError(t, err)

	patch := etre.Entity{"y": "y"} // y=a -> y=y
	wo1 := entity.WriteOp{
		EntityType: entityType,
		Caller:     username,
		SetOp:      "update-y1",
		SetId:      "111",
		SetSize:    1,
	}
	gotDiffs, err := store.UpdateEntities(wo1, q, patch)
	require.NoError(t, err)
	assert.Len(t, gotDiffs, 1)
	expectDiffs := []etre.Entity{
		{
			"_id":   testNodes[0]["_id"],
			"_type": entityType,
			"_rev":  int64(0),
			"y":     "a",
		},
	}
	assert.Equal(t, expectDiffs, gotDiffs)

	// ----------------------------------------------------------------------
	// And this matches 2nd and 3rd test nodes
	q, err = query.Translate("y=b")
	require.NoError(t, err)

	patch = etre.Entity{"y": "c"} // y=b -> y=c
	wo2 := entity.WriteOp{
		EntityType: entityType,
		Caller:     username,
		SetOp:      "update-y2",
		SetId:      "222",
		SetSize:    1,
	}
	gotDiffs, err = store.UpdateEntities(wo2, q, patch)
	require.NoError(t, err)
	assert.Len(t, gotDiffs, 2)
	expectDiffs = []etre.Entity{
		{
			"_id":   testNodes[1]["_id"],
			"_type": entityType,
			"_rev":  int64(0),
			"y":     "b",
		},
		{
			"_id":   testNodes[2]["_id"],
			"_type": entityType,
			"_rev":  int64(0),
			"y":     "b",
		},
	}
	assert.Equal(t, expectDiffs, gotDiffs)

	// ----------------------------------------------------------------------
	// 3 CDC events because 3 entities were updated
	for i := range gotEvents {
		gotEvents[i].Id = ""
		gotEvents[i].Ts = 0
	}
	id1, _ := testNodes[0]["_id"].(primitive.ObjectID)
	id2, _ := testNodes[1]["_id"].(primitive.ObjectID)
	id3, _ := testNodes[2]["_id"].(primitive.ObjectID)
	expectEvent := []etre.CDCEvent{
		{
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &etre.Entity{"y": "a"},
			New:        &etre.Entity{"y": "y"},
			SetId:      "111",
			SetOp:      "update-y1",
			SetSize:    1,
		},
		{
			EntityId:   id2.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &etre.Entity{"y": "b"},
			New:        &etre.Entity{"y": "c"},
			SetId:      "222",
			SetOp:      "update-y2",
			SetSize:    1,
		},
		{
			EntityId:   id3.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &etre.Entity{"y": "b"},
			New:        &etre.Entity{"y": "c"},
			SetId:      "222",
			SetOp:      "update-y2",
			SetSize:    1,
		},
	}
	assert.Equal(t, expectEvent, gotEvents)
}

func TestUpdateEntitiesById(t *testing.T) {
	// Test that an update by object ID works. In the test above, we look up
	// label y and also change it: y=a -> y=y. So the store can loop over calls
	// to FindOneAndUpdate() and the loop is self-terminating as it changes
	// all y=a to y=y. But if we look up _id and chagne y, the loop become infinite
	// because it'll keep finding _id.
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setup(t, cdcm)

	// ----------------------------------------------------------------------
	// This matches first test node
	q, _ := query.Translate("_id=" + testNodes[0]["_id"].(primitive.ObjectID).Hex())
	patch := etre.Entity{"y": "y"} // y=a -> y=y
	wo1 := entity.WriteOp{
		EntityType: entityType,
		Caller:     username,
		SetOp:      "update-y1",
		SetId:      "111",
		SetSize:    1,
	}
	gotDiffs, err := store.UpdateEntities(wo1, q, patch)
	require.NoError(t, err)
	expectDiffs := []etre.Entity{
		{
			"_id":   testNodes[0]["_id"],
			"_type": entityType,
			"_rev":  int64(0),
			"y":     "a",
		},
	}
	assert.Equal(t, expectDiffs, gotDiffs)

	// ----------------------------------------------------------------------
	// And this matches 2nd and 3rd test nodes
	q, err = query.Translate("y=b")
	require.NoError(t, err)

	patch = etre.Entity{"y": "c"} // y=b -> y=c
	wo2 := entity.WriteOp{
		EntityType: entityType,
		Caller:     username,
		SetOp:      "update-y2",
		SetId:      "222",
		SetSize:    1,
	}
	gotDiffs, err = store.UpdateEntities(wo2, q, patch)
	require.NoError(t, err)
	expectDiffs = []etre.Entity{
		{
			"_id":   testNodes[1]["_id"],
			"_type": entityType,
			"_rev":  int64(0),
			"y":     "b",
		},
		{
			"_id":   testNodes[2]["_id"],
			"_type": entityType,
			"_rev":  int64(0),
			"y":     "b",
		},
	}
	assert.Equal(t, expectDiffs, gotDiffs)

	// ----------------------------------------------------------------------
	// 3 CDC events because 3 entities were updated
	for i := range gotEvents {
		gotEvents[i].Id = ""
		gotEvents[i].Ts = 0
	}
	id1, _ := testNodes[0]["_id"].(primitive.ObjectID)
	id2, _ := testNodes[1]["_id"].(primitive.ObjectID)
	id3, _ := testNodes[2]["_id"].(primitive.ObjectID)
	expectEvent := []etre.CDCEvent{
		{
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &etre.Entity{"y": "a"},
			New:        &etre.Entity{"y": "y"},
			SetId:      "111",
			SetOp:      "update-y1",
			SetSize:    1,
		},
		{
			EntityId:   id2.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &etre.Entity{"y": "b"},
			New:        &etre.Entity{"y": "c"},
			SetId:      "222",
			SetOp:      "update-y2",
			SetSize:    1,
		},
		{
			EntityId:   id3.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &etre.Entity{"y": "b"},
			New:        &etre.Entity{"y": "c"},
			SetId:      "222",
			SetOp:      "update-y2",
			SetSize:    1,
		},
	}
	assert.Equal(t, expectEvent, gotEvents)
}

func TestUpdateEntitiesDuplicate(t *testing.T) {
	// Test that dupes are handled on update. There's a uniqe index on x.
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setup(t, cdcm)

	// This matches first test node
	q, err := query.Translate("y=a")
	require.NoError(t, err)

	patch := etre.Entity{"x": 6} // x=2 -> x=6 conflicts with 3rd test node
	wo1 := entity.WriteOp{
		EntityType: entityType,
		Caller:     username,
	}
	gotDiffs, err := store.UpdateEntities(wo1, q, patch)
	require.Error(t, err)
	dberr, ok := err.(entity.DbError)
	require.True(t, ok, "got error type %#v, expected entity.DbError", err)
	assert.Equal(t, "duplicate-entity", dberr.Type)
	assert.Empty(t, gotDiffs)
	assert.Empty(t, gotEvents)
}

// --------------------------------------------------------------------------
// Delete
// --------------------------------------------------------------------------

func TestDeleteEntities(t *testing.T) {
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setup(t, cdcm)

	// Match one first test node
	q, err := query.Translate("y == a")
	require.NoError(t, err)

	gotOld, err := store.DeleteEntities(wo, q)
	require.NoError(t, err)
	assert.Equal(t, testNodes[:1], gotOld)

	// Match last two test nodes
	q, err = query.Translate("y == b")
	require.NoError(t, err)

	gotOld, err = store.DeleteEntities(wo, q)
	require.NoError(t, err)
	assert.Equal(t, testNodes[1:], gotOld)

	for i := range gotEvents {
		gotEvents[i].Id = ""
		gotEvents[i].Ts = 0
	}
	id1, _ := testNodes[0]["_id"].(primitive.ObjectID)
	id2, _ := testNodes[1]["_id"].(primitive.ObjectID)
	id3, _ := testNodes[2]["_id"].(primitive.ObjectID)
	expectEvent := []etre.CDCEvent{
		{
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "d",
			Old:        &testNodes[0],
		},
		{
			EntityId:   id2.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "d",
			Old:        &testNodes[1],
		},
		{
			EntityId:   id3.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "d",
			Old:        &testNodes[2],
		},
	}
	assert.Equal(t, expectEvent, gotEvents)
}

// --------------------------------------------------------------------------
// Delete Label
// --------------------------------------------------------------------------

func TestDeleteLabel(t *testing.T) {
	gotEvents := []etre.CDCEvent{}
	cdcm := &mock.CDCStore{
		WriteFunc: func(ctx context.Context, e etre.CDCEvent) error {
			gotEvents = append(gotEvents, e)
			return nil
		},
	}
	store := setup(t, cdcm)

	wo := entity.WriteOp{
		EntityType: entityType,
		EntityId:   testNodes[0]["_id"].(primitive.ObjectID).Hex(),
		Caller:     username,
	}
	gotOld, err := store.DeleteLabel(wo, "foo")
	require.NoError(t, err)

	expectOld := etre.Entity{
		"_id":   testNodes[0]["_id"],
		"_type": testNodes[0]["_type"],
		"_rev":  testNodes[0]["_rev"],
		"foo":   "",
	}
	assert.Equal(t, expectOld, gotOld)

	// The foo label should no longer be set on the entity
	q, _ := query.Translate("y=a")
	gotNew, err := store.ReadEntities(entityType, q, etre.QueryFilter{})
	require.NoError(t, err)

	e := etre.Entity{}
	for k, v := range testNodes[0] {
		e[k] = v
	}
	delete(e, "foo")                  // because we deleted the label
	e["_rev"] = e["_rev"].(int64) + 1 // because we deleted the label
	expectNew := []etre.Entity{e}
	assert.Equal(t, expectNew, gotNew)

	for i := range gotEvents {
		gotEvents[i].Id = ""
		gotEvents[i].Ts = 0
	}
	id1, _ := testNodes[0]["_id"].(primitive.ObjectID)
	expectedEventNew := etre.Entity{
		"_id":   testNodes[0]["_id"],
		"_type": testNodes[0]["_type"],
		"_rev":  e["_rev"],
	}
	expectEvent := []etre.CDCEvent{
		{
			EntityId:   id1.Hex(),
			EntityType: entityType,
			EntityRev:  int64(1),
			Caller:     username,
			Op:         "u",
			Old:        &expectOld,
			New:        &expectedEventNew,
		},
	}
	assert.Equal(t, expectEvent, gotEvents)
}
