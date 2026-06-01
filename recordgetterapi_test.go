package main

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	pbgd "github.com/brotherlogic/godiscogs/proto"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
)

type testGetter struct {
	records           []*pbrc.Record
	fail              bool
	nopile            bool
	failGetInCategory bool
	failGetInFolder   bool
}

func (tg *testGetter) getRecords(ctx context.Context, folderID int32) (*pbrc.GetRecordsResponse, error) {
	if tg.fail {
		return nil, fmt.Errorf("Built to Fail")
	}
	if tg.nopile && folderID == 812802 {
		return &pbrc.GetRecordsResponse{}, nil
	}
	return &pbrc.GetRecordsResponse{Records: tg.records}, nil
}
func (tg *testGetter) getRelease(ctx context.Context, instanceID int64) (*pbrc.Record, error) {
	if len(tg.records) > 0 {
		return tg.records[0], nil
	}
	return nil, nil
}

func (tg *testGetter) getAuditionRelease(ctx context.Context) (*pbrc.Record, error) {
	if len(tg.records) > 0 {
		return tg.records[0], nil
	}
	return nil, nil
}

func (tg *testGetter) getPlainRecord(ctx context.Context, id int32) (*pbrc.Record, error) {
	if len(tg.records) > 0 {
		return tg.records[0], nil
	}
	return nil, nil
}

func (tg *testGetter) getRecordsInCategory(ctx context.Context, category pbrc.ReleaseMetadata_Category) ([]int64, error) {
	if tg.failGetInCategory {
		return []int64{}, fmt.Errorf("Built to fail")
	}
	return []int64{1}, nil
}

func (tg *testGetter) getRecordsInFolder(ctx context.Context, folder int32) ([]int64, error) {
	if tg.failGetInFolder {
		return []int64{}, fmt.Errorf("Built to fail")
	}
	return []int64{1}, nil
}

type priorityTestGetter struct {
	records     map[int64]*pbrc.Record
	categoryIDs map[pbrc.ReleaseMetadata_Category][]int64
}

func (p *priorityTestGetter) getRelease(ctx context.Context, id int64) (*pbrc.Record, error) {
	if rec, ok := p.records[id]; ok {
		return rec, nil
	}
	return nil, fmt.Errorf("record not found")
}

func (p *priorityTestGetter) getRecordsInCategory(ctx context.Context, cat pbrc.ReleaseMetadata_Category) ([]int64, error) {
	return p.categoryIDs[cat], nil
}

func (p *priorityTestGetter) getRecordsInFolder(ctx context.Context, folder int32) ([]int64, error) {
	return nil, nil
}

func (p *priorityTestGetter) getPlainRecord(ctx context.Context, id int32) (*pbrc.Record, error) {
	return p.getRelease(ctx, int64(id))
}

func (p *priorityTestGetter) getAuditionRelease(ctx context.Context) (*pbrc.Record, error) {
	return nil, nil
}

func makeDigitalRecord(id int64, category pbrc.ReleaseMetadata_Category) *pbrc.Record {
	return &pbrc.Record{
		Release: &pbgd.Release{
			InstanceId: id,
			FolderId:   812802,
			Rating:     0,
		},
		Metadata: &pbrc.ReleaseMetadata{
			DateArrived:     100,
			NeedsGramUpdate: false,
			FiledUnder:      pbrc.ReleaseMetadata_FILE_DIGITAL,
			Category:        category,
			Dirty:           false,
			SetRating:       0,
			GoalFolder:      268147,
			BoxState:        pbrc.ReleaseMetadata_BOX_UNKNOWN,
			DateAdded:       1000,
		},
	}
}

func TestGetFromDigital(t *testing.T) {
	s := InitTestServer()

	recUnlistened := makeDigitalRecord(1, pbrc.ReleaseMetadata_UNLISTENED)
	recStagedToSell := makeDigitalRecord(2, pbrc.ReleaseMetadata_STAGED_TO_SELL)
	recPreInCollection := makeDigitalRecord(3, pbrc.ReleaseMetadata_PRE_IN_COLLECTION)
	recPreHighSchool := makeDigitalRecord(4, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL)

	records := map[int64]*pbrc.Record{
		1: recUnlistened,
		2: recStagedToSell,
		3: recPreInCollection,
		4: recPreHighSchool,
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_UNLISTENED:        {1},
		pbrc.ReleaseMetadata_STAGED_TO_SELL:    {2},
		pbrc.ReleaseMetadata_PRE_IN_COLLECTION: {3},
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL:   {4},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	// 1. All categories are available. Should prioritize UNLISTENED (ID 1).
	rec, err := s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 1 {
		t.Errorf("Expected UNLISTENED record (ID 1) to be prioritized, got: %v", rec)
	}

	// 2. UNLISTENED is not available. Should prioritize STAGED_TO_SELL (ID 2).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_UNLISTENED] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 2 {
		t.Errorf("Expected STAGED_TO_SELL record (ID 2) to be prioritized, got: %v", rec)
	}

	// 3. UNLISTENED and STAGED_TO_SELL are not available. Should prioritize PRE_IN_COLLECTION (ID 3).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_STAGED_TO_SELL] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 3 {
		t.Errorf("Expected PRE_IN_COLLECTION record (ID 3) to be prioritized, got: %v", rec)
	}

	// 4. Only PRE_HIGH_SCHOOL is available. Should pick PRE_HIGH_SCHOOL (ID 4).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_PRE_IN_COLLECTION] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 4 {
		t.Errorf("Expected PRE_HIGH_SCHOOL record (ID 4) to be prioritized, got: %v", rec)
	}

	// 5. None are available. Should return an error.
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err == nil {
		t.Errorf("Expected error when no digital records are available, but got: %v", rec)
	}
}

func makeCDRecord(id int64, category pbrc.ReleaseMetadata_Category) *pbrc.Record {
	return &pbrc.Record{
		Release: &pbgd.Release{
			InstanceId: id,
			FolderId:   812802,
			Rating:     0,
		},
		Metadata: &pbrc.ReleaseMetadata{
			DateArrived:     100,
			NeedsGramUpdate: false,
			FiledUnder:      pbrc.ReleaseMetadata_FILE_CD,
			Category:        category,
			Dirty:           false,
			SetRating:       0,
			GoalFolder:      268147,
			BoxState:        pbrc.ReleaseMetadata_BOX_UNKNOWN,
			DateAdded:       1000,
		},
	}
}

func TestGetFromCD(t *testing.T) {
	s := InitTestServer()

	recUnlistened := makeCDRecord(1, pbrc.ReleaseMetadata_UNLISTENED)
	recStagedToSell := makeCDRecord(2, pbrc.ReleaseMetadata_STAGED_TO_SELL)
	recPreInCollection := makeCDRecord(3, pbrc.ReleaseMetadata_PRE_IN_COLLECTION)
	recPreHighSchool := makeCDRecord(4, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL)

	records := map[int64]*pbrc.Record{
		1: recUnlistened,
		2: recStagedToSell,
		3: recPreInCollection,
		4: recPreHighSchool,
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_UNLISTENED:        {1},
		pbrc.ReleaseMetadata_STAGED_TO_SELL:    {2},
		pbrc.ReleaseMetadata_PRE_IN_COLLECTION: {3},
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL:   {4},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	// 1. All categories are available. Should prioritize UNLISTENED (ID 1).
	rec, err := s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_CD_FOCUS)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 1 {
		t.Errorf("Expected UNLISTENED record (ID 1) to be prioritized, got: %v", rec)
	}

	// 2. UNLISTENED is not available. Should prioritize STAGED_TO_SELL (ID 2).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_UNLISTENED] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_CD_FOCUS)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 2 {
		t.Errorf("Expected STAGED_TO_SELL record (ID 2) to be prioritized, got: %v", rec)
	}

	// 3. UNLISTENED and STAGED_TO_SELL are not available. Should prioritize PRE_IN_COLLECTION (ID 3).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_STAGED_TO_SELL] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_CD_FOCUS)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 3 {
		t.Errorf("Expected PRE_IN_COLLECTION record (ID 3) to be prioritized, got: %v", rec)
	}

	// 4. Only PRE_HIGH_SCHOOL is available. Should pick PRE_HIGH_SCHOOL (ID 4).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_PRE_IN_COLLECTION] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_CD_FOCUS)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 4 {
		t.Errorf("Expected PRE_HIGH_SCHOOL record (ID 4) to be prioritized, got: %v", rec)
	}

	// 5. None are available. Should return an error.
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_CD_FOCUS)
	if err == nil {
		t.Errorf("Expected error when no CD records are available, but got: %v", rec)
	}
}

func TestForce(t *testing.T) {
	s := InitTestServer()

	_, err := s.Force(context.Background(), &pb.ForceRequest{})

	if err != nil {
		t.Errorf("Error forcing: %v", err)
	}

}
