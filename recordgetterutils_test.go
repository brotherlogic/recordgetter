package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/brotherlogic/keystore/client"
	"golang.org/x/net/context"

	pbgd "github.com/brotherlogic/godiscogs"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
)

func InitTestServer() *Server {
	s := Init()
	s.SkipLog = true
	s.GoServer.KSclient = *keystoreclient.GetTestClient(".test")
	s.rGetter = &testGetter{}
	return s
}

type testUpdater struct {
	lastScore int32
	fail      bool
}

func (t *testUpdater) update(ctx context.Context, rec *pbrc.Record) error {
	if t.fail {
		return fmt.Errorf("Build to fail")
	}
	t.lastScore = rec.Release.Rating
	return nil
}

func TestFullScore(t *testing.T) {
	s := InitTestServer()
	updater := &testUpdater{}
	s.updater = updater
	s.state.Scores = append(s.state.Scores, &pb.DiskScore{InstanceId: 1234, DiskNumber: 1, Score: 2})
	s.state.Scores = append(s.state.Scores, &pb.DiskScore{InstanceId: 123224, DiskNumber: 1, Score: 2})

	s.Listened(context.Background(), &pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, Rating: 5, FormatQuantity: 2}})

	if updater.lastScore != 4 {
		t.Errorf("Update has not combined scores: %v", updater.lastScore)
	}

	if len(s.state.Scores) != 1 {
		t.Errorf("Scores have not been removed: %v", s.state.Scores)
	}
}

func TestPartialScore(t *testing.T) {
	s := InitTestServer()
	updater := &testUpdater{}
	s.updater = updater
	s.state.Scores = append(s.state.Scores, &pb.DiskScore{InstanceId: 12, DiskNumber: 1, Score: 2})

	s.Listened(context.Background(), &pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, Rating: 5, FormatQuantity: 2}})

	if updater.lastScore != 0 {
		t.Errorf("Score has been set despite disk missing: %v", updater.lastScore)
	}

	if len(s.state.Scores) != 2 {
		t.Errorf("Disk score has not been added!: %v", s.state.Scores)
	}
}

func TestNeedsRip(t *testing.T) {
	s := InitTestServer()
	nr := s.needsRip(&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{}, Release: &pbgd.Release{InstanceId: 1234, Formats: []*pbgd.Format{&pbgd.Format{Name: "CD"}}}})

	if !nr {
		t.Errorf("Should be reported as nedding a riop")
	}
}

func TestNeedsRipDigital(t *testing.T) {
	s := InitTestServer()
	nr := s.needsRip(&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{GoalFolder: 268147}, Release: &pbgd.Release{InstanceId: 1234, Formats: []*pbgd.Format{&pbgd.Format{Name: "CD"}}}})

	if nr {
		t.Errorf("Digital records don't need rip")
	}
}

func TestNotNeedsRip(t *testing.T) {
	s := InitTestServer()
	nr := s.needsRip(&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{CdPath: "blah"}, Release: &pbgd.Release{InstanceId: 1234, Formats: []*pbgd.Format{&pbgd.Format{Name: "CD"}}}})

	if nr {
		t.Errorf("Should be reported as nedding a riop")
	}
}

func TestNumberListens(t *testing.T) {
	if getNumListens(&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN}}) != 3 {
		t.Errorf("Bad number of listens")
	}
}

func TestGetPreFreshamanOnCategoryGet(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{CdPath: "blah"}, Release: &pbgd.Release{InstanceId: 1}}}}

	rec, err := s.getCategoryRecord(context.Background(), time.Now(), pbrc.ReleaseMetadata_PRE_FRESHMAN)
	if err != nil {
		t.Errorf("Did not fail: %v", err)
	}

	if rec == nil {
		t.Errorf("No record returned")
	}
}

func TestFailFailOnCategoryGet(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{failGetInCategory: true}

	rec, err := s.getCategoryRecord(context.Background(), time.Now(), pbrc.ReleaseMetadata_PRE_FRESHMAN)
	if err == nil {
		t.Errorf("Did not fail: %v", rec)
	}
}

func TestCategoryEmpty(t *testing.T) {
	s := InitTestServer()

	rec, err := s.getCategoryRecord(context.Background(), time.Now(), pbrc.ReleaseMetadata_PRE_FRESHMAN)
	if err != nil || rec != nil {
		t.Errorf("Did not fail: %v -> %v", rec, err)
	}
}

func TestGetInFolderFailOnCategoryGet(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{failGetInFolder: true}

	rec, err := s.getInFolderWithCategory(context.Background(), time.Now(), int32(12), pbrc.ReleaseMetadata_PRE_FRESHMAN)

	if err == nil {
		t.Errorf("Did not fail: %v", rec)
	}
}

func TestGetInFolderWithCategoryEmpty(t *testing.T) {
	s := InitTestServer()

	rec, err := s.getInFolderWithCategory(context.Background(), time.Now(), int32(12), pbrc.ReleaseMetadata_PRE_FRESHMAN)

	if err != nil || rec != nil {
		t.Errorf("Did not fail: %v -> %v", rec, err)
	}
}

func TestGetInFolderWithCategory(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{CdPath: "blah", Category: pbrc.ReleaseMetadata_PRE_FRESHMAN}, Release: &pbgd.Release{InstanceId: 1}}}}

	rec, err := s.getInFolderWithCategory(context.Background(), time.Now(), int32(12), pbrc.ReleaseMetadata_PRE_FRESHMAN)
	if err != nil {
		t.Errorf("Did not fail: %v", err)
	}

	if rec == nil {
		t.Errorf("No record returned")
	}
}

func TestGetInFolder(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{CdPath: "blah", Category: pbrc.ReleaseMetadata_PRE_FRESHMAN}, Release: &pbgd.Release{InstanceId: 1}}}}

	rec, err := s.getInFolders(context.Background(), time.Now(), []int32{12})
	if err != nil {
		t.Errorf("Did not fail: %v", err)
	}

	if rec == nil {
		t.Errorf("No record returned")
	}
}

func TestGetInFolderEmpty(t *testing.T) {
	s := InitTestServer()

	rec, err := s.getInFolders(context.Background(), time.Now(), []int32{12})
	if err != nil {
		t.Errorf("Did not fail: %v", err)
	}

	if rec != nil {
		t.Errorf("Record returned: %v", rec)
	}
}

func TestGetInFolderFail(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{failGetInFolder: true}

	rec, err := s.getInFolders(context.Background(), time.Now(), []int32{12})
	if err == nil {
		t.Errorf("Did not fail: %v", rec)
	}

}
