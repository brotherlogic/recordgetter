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
	recPreValidate := makeDigitalRecord(5, pbrc.ReleaseMetadata_PRE_VALIDATE)

	records := map[int64]*pbrc.Record{
		1: recUnlistened,
		2: recStagedToSell,
		3: recPreInCollection,
		4: recPreHighSchool,
		5: recPreValidate,
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_UNLISTENED:        {1},
		pbrc.ReleaseMetadata_STAGED_TO_SELL:    {2},
		pbrc.ReleaseMetadata_PRE_IN_COLLECTION: {3},
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL:   {4},
		pbrc.ReleaseMetadata_PRE_VALIDATE:      {5},
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

	// 2. UNLISTENED is not available. Should prioritize PRE_HIGH_SCHOOL (ID 4).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_UNLISTENED] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 4 {
		t.Errorf("Expected PRE_HIGH_SCHOOL record (ID 4) to be prioritized, got: %v", rec)
	}

	// 3. UNLISTENED and PRE_HIGH_SCHOOL are not available. Should prioritize PRE_IN_COLLECTION (ID 3).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 3 {
		t.Errorf("Expected PRE_IN_COLLECTION record (ID 3) to be prioritized, got: %v", rec)
	}

	// 4. UNLISTENED, PRE_HIGH_SCHOOL, and PRE_IN_COLLECTION are not available. Should pick STAGED_TO_SELL (ID 2).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_PRE_IN_COLLECTION] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 2 {
		t.Errorf("Expected STAGED_TO_SELL record (ID 2) to be prioritized, got: %v", rec)
	}

	// 5. Only PRE_VALIDATE is available. Should pick PRE_VALIDATE (ID 5).
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_STAGED_TO_SELL] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 5 {
		t.Errorf("Expected PRE_VALIDATE record (ID 5) to be prioritized, got: %v", rec)
	}

	// 6. None are available. Should return an error.
	s.rGetter.(*priorityTestGetter).categoryIDs[pbrc.ReleaseMetadata_PRE_VALIDATE] = []int64{}
	rec, err = s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err == nil {
		t.Errorf("Expected error when no digital records are available, but got: %v", rec)
	}
}

func TestGetFromDigitalWithCD(t *testing.T) {
	s := InitTestServer()

	// Create one CD and one DIGITAL record in UNLISTENED
	recCD := makeCDRecord(1, pbrc.ReleaseMetadata_UNLISTENED)
	recDigital := makeDigitalRecord(2, pbrc.ReleaseMetadata_UNLISTENED)

	records := map[int64]*pbrc.Record{
		1: recCD,
		2: recDigital,
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_UNLISTENED: {1, 2},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	// We should be able to get a record, and it can be a CD record (ID 1)
	rec, err := s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error picking digital/CD: %v", err)
	}
	if rec == nil {
		t.Errorf("Expected a record, got nil")
	}
}

func TestGetFromDigitalPrioritizesForSaleCD(t *testing.T) {
	s := InitTestServer()

	// 1. Create a regular digital record in UNLISTENED (usually prioritized first)
	recDigital := makeDigitalRecord(2, pbrc.ReleaseMetadata_UNLISTENED)

	// 2. Create a CD record in PRE_HIGH_SCHOOL (a category usually checked later)
	// but make it FOR_SALE
	recCD := makeCDRecord(1, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL)
	recCD.Metadata.SaleState = pbgd.SaleState_FOR_SALE

	records := map[int64]*pbrc.Record{
		1: recCD,
		2: recDigital,
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_UNLISTENED:      {2},
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL: {1},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	// We should pick the FOR_SALE CD (ID 1) even though there is an UNLISTENED digital record (ID 2).
	rec, err := s.getReleaseFromPile(context.Background(), &pb.State{}, time.Now(), pb.RequestType_DIGITAL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 1 {
		t.Errorf("Expected FOR_SALE CD (ID 1) to be prioritized, got: %v", rec)
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

func makeVinylRecord(id int64, category pbrc.ReleaseMetadata_Category, filedUnder pbrc.ReleaseMetadata_FileSize) *pbrc.Record {
	return &pbrc.Record{
		Release: &pbgd.Release{
			InstanceId: id,
			FolderId:   812802,
			Rating:     0,
		},
		Metadata: &pbrc.ReleaseMetadata{
			DateArrived:     100,
			NeedsGramUpdate: false,
			FiledUnder:      filedUnder,
			Category:        category,
			Dirty:           false,
			SetRating:       0,
			GoalFolder:      242017,
			BoxState:        pbrc.ReleaseMetadata_BOX_UNKNOWN,
			DateAdded:       1000,
		},
	}
}

func TestGetFromDefaultPriorityOrder(t *testing.T) {
	s := InitTestServer()

	records := map[int64]*pbrc.Record{
		1:  makeVinylRecord(1, pbrc.ReleaseMetadata_UNLISTENED, pbrc.ReleaseMetadata_FILE_12_INCH),
		2:  makeVinylRecord(2, pbrc.ReleaseMetadata_UNLISTENED, pbrc.ReleaseMetadata_FILE_7_INCH),
		3:  makeVinylRecord(3, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_12_INCH),
		4:  makeVinylRecord(4, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_7_INCH),
		5:  makeVinylRecord(5, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_12_INCH),
		6:  makeVinylRecord(6, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_7_INCH),
		7:  makeVinylRecord(7, pbrc.ReleaseMetadata_STAGED_TO_SELL, pbrc.ReleaseMetadata_FILE_12_INCH),
		8:  makeVinylRecord(8, pbrc.ReleaseMetadata_STAGED_TO_SELL, pbrc.ReleaseMetadata_FILE_7_INCH),
		9:  makeVinylRecord(9, pbrc.ReleaseMetadata_PRE_VALIDATE, pbrc.ReleaseMetadata_FILE_12_INCH),
		10: makeVinylRecord(10, pbrc.ReleaseMetadata_PRE_VALIDATE, pbrc.ReleaseMetadata_FILE_7_INCH),
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_UNLISTENED:        {1, 2},
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL:   {3, 4},
		pbrc.ReleaseMetadata_PRE_IN_COLLECTION: {5, 6},
		pbrc.ReleaseMetadata_STAGED_TO_SELL:    {7, 8},
		pbrc.ReleaseMetadata_PRE_VALIDATE:      {9, 10},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	expectedOrder := []int64{3, 4, 5, 6, 1, 2, 7, 8, 9, 10}
	state := &pb.State{CattypeCount: make(map[string]int32)}

	// Ensure time is set to a non-December month for testing STAGED_TO_SELL
	testTime := time.Date(2026, time.June, 1, 12, 0, 0, 0, time.UTC)

	for _, expectedID := range expectedOrder {
		rec, err := s.getReleaseFromPile(context.Background(), state, testTime, pb.RequestType_DEFAULT)
		if err != nil {
			t.Fatalf("Unexpected error picking record (expected ID %d): %v", expectedID, err)
		}
		if rec == nil || rec.GetRelease().GetInstanceId() != expectedID {
			t.Fatalf("Expected record ID %d, got: %v", expectedID, rec)
		}

		// Remove the picked record from category list
		cat := records[expectedID].GetMetadata().GetCategory()
		var remaining []int64
		for _, id := range s.rGetter.(*priorityTestGetter).categoryIDs[cat] {
			if id != expectedID {
				remaining = append(remaining, id)
			}
		}
		s.rGetter.(*priorityTestGetter).categoryIDs[cat] = remaining
	}
}

func TestGetFromDefaultWithCattypeCount(t *testing.T) {
	s := InitTestServer()

	// Provide records for UNLISTENED (12" & 7"), PHS (12" & 7"), PIC (12" & 7"), STS (12"), PV (12")
	records := map[int64]*pbrc.Record{
		1: makeVinylRecord(1, pbrc.ReleaseMetadata_UNLISTENED, pbrc.ReleaseMetadata_FILE_12_INCH),
		2: makeVinylRecord(2, pbrc.ReleaseMetadata_UNLISTENED, pbrc.ReleaseMetadata_FILE_7_INCH),
		3: makeVinylRecord(3, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_12_INCH),
		4: makeVinylRecord(4, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_7_INCH),
		5: makeVinylRecord(5, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_12_INCH),
		6: makeVinylRecord(6, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_7_INCH),
		7: makeVinylRecord(7, pbrc.ReleaseMetadata_STAGED_TO_SELL, pbrc.ReleaseMetadata_FILE_12_INCH),
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_UNLISTENED:        {1, 2},
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL:   {3, 4},
		pbrc.ReleaseMetadata_PRE_IN_COLLECTION: {5, 6},
		pbrc.ReleaseMetadata_STAGED_TO_SELL:    {7},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	// State already has 1 PHS 12" and 1 PIC 12" today
	state := &pb.State{
		CattypeCount: map[string]int32{
			fmt.Sprintf("%v%v", pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_12_INCH):   1,
			fmt.Sprintf("%v%v", pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_12_INCH): 1,
		},
	}

	testTime := time.Date(2026, time.June, 1, 12, 0, 0, 0, time.UTC)

	// Since PHS 12" count is > 0, should pick PHS 7" (ID 4)
	rec, err := s.getReleaseFromPile(context.Background(), state, testTime, pb.RequestType_DEFAULT)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 4 {
		t.Fatalf("Expected PHS 7\" (ID 4), got: %v", rec)
	}

	// Now set PHS 7" count to 1 as well; since PIC 12" has count 1, it should pick PIC 7" (ID 6)
	state.CattypeCount[fmt.Sprintf("%v%v", pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_7_INCH)] = 1
	rec, err = s.getReleaseFromPile(context.Background(), state, testTime, pb.RequestType_DEFAULT)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 6 {
		t.Fatalf("Expected PIC 7\" (ID 6), got: %v", rec)
	}

	// Now set PIC 7" count to 1 as well; now all 4 have count >= 1. It should pick UNLISTENED 12" (ID 1)
	state.CattypeCount[fmt.Sprintf("%v%v", pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_7_INCH)] = 1
	rec, err = s.getReleaseFromPile(context.Background(), state, testTime, pb.RequestType_DEFAULT)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 1 {
		t.Fatalf("Expected UNLISTENED 12\" (ID 1), got: %v", rec)
	}
}

func TestSequentialPicksCattypeFlow(t *testing.T) {
	s := InitTestServer()

	// 2 PHS 12", 2 PHS 7", 2 PIC 12", 2 PIC 7", 2 UNLISTENED 12"
	records := map[int64]*pbrc.Record{
		1:  makeVinylRecord(1, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_12_INCH),
		2:  makeVinylRecord(2, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_12_INCH),
		3:  makeVinylRecord(3, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_7_INCH),
		4:  makeVinylRecord(4, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_7_INCH),
		5:  makeVinylRecord(5, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_12_INCH),
		6:  makeVinylRecord(6, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_12_INCH),
		7:  makeVinylRecord(7, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_7_INCH),
		8:  makeVinylRecord(8, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_7_INCH),
		9:  makeVinylRecord(9, pbrc.ReleaseMetadata_UNLISTENED, pbrc.ReleaseMetadata_FILE_12_INCH),
		10: makeVinylRecord(10, pbrc.ReleaseMetadata_UNLISTENED, pbrc.ReleaseMetadata_FILE_12_INCH),
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL:   {1, 2, 3, 4},
		pbrc.ReleaseMetadata_PRE_IN_COLLECTION: {5, 6, 7, 8},
		pbrc.ReleaseMetadata_UNLISTENED:        {9, 10},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	state := &pb.State{CattypeCount: make(map[string]int32)}
	testTime := time.Date(2026, time.June, 1, 12, 0, 0, 0, time.UTC)

	// Expected sequence:
	// 1. First PHS 12" (ID 1)
	// 2. First PHS 7" (ID 3)
	// 3. First PIC 12" (ID 5)
	// 4. First PIC 7" (ID 7)
	// 5. UNLISTENED 12" (ID 9)
	// 6. UNLISTENED 12" (ID 10)
	expectedSequence := []int64{1, 3, 5, 7, 9, 10}

	for _, expectedID := range expectedSequence {
		rec, err := s.getReleaseFromPile(context.Background(), state, testTime, pb.RequestType_DEFAULT)
		if err != nil {
			t.Fatalf("Unexpected error picking record (expected ID %d): %v", expectedID, err)
		}
		if rec == nil || rec.GetRelease().GetInstanceId() != expectedID {
			t.Fatalf("Expected record ID %d, got: %v", expectedID, rec)
		}

		// Update CattypeCount as GetRecord would
		state.CattypeCount[fmt.Sprintf("%v%v", rec.GetMetadata().GetCategory(), rec.GetMetadata().GetFiledUnder())]++

		// Remove the picked record from category list
		cat := records[expectedID].GetMetadata().GetCategory()
		var remaining []int64
		for _, id := range s.rGetter.(*priorityTestGetter).categoryIDs[cat] {
			if id != expectedID {
				remaining = append(remaining, id)
			}
		}
		s.rGetter.(*priorityTestGetter).categoryIDs[cat] = remaining
	}
}

func TestForce(t *testing.T) {
	s := InitTestServer()

	_, err := s.Force(context.Background(), &pb.ForceRequest{})

	if err != nil {
		t.Errorf("Error forcing: %v", err)
	}

}

func TestCassetteNotPickedForSevenInch(t *testing.T) {
	s := InitTestServer()

	// Only cassette in PRE_HIGH_SCHOOL, 7" in PRE_IN_COLLECTION
	records := map[int64]*pbrc.Record{
		1: makeVinylRecord(1, pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, pbrc.ReleaseMetadata_FILE_TAPE),
		2: makeVinylRecord(2, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, pbrc.ReleaseMetadata_FILE_7_INCH),
	}

	categoryIDs := map[pbrc.ReleaseMetadata_Category][]int64{
		pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL:   {1},
		pbrc.ReleaseMetadata_PRE_IN_COLLECTION: {2},
	}

	s.rGetter = &priorityTestGetter{
		records:     records,
		categoryIDs: categoryIDs,
	}

	state := &pb.State{CattypeCount: make(map[string]int32)}
	testTime := time.Date(2026, time.June, 1, 12, 0, 0, 0, time.UTC)

	// Since PHS only has FILE_TAPE, PHS 12" and PHS 7" will find nothing.
	// It should skip PHS and pick PIC 7" (ID 2).
	rec, err := s.getReleaseFromPile(context.Background(), state, testTime, pb.RequestType_DEFAULT)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec == nil || rec.GetRelease().GetInstanceId() != 2 {
		t.Fatalf("Expected PIC 7\" (ID 2) to be picked instead of cassette, got: %v", rec)
	}
}
