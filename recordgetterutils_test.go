package main

import (
	"fmt"
	"testing"
	"time"

	keystoreclient "github.com/brotherlogic/keystore/client"
	pb "github.com/brotherlogic/recordgetter/proto"
	"golang.org/x/net/context"

	pbgd "github.com/brotherlogic/godiscogs/proto"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbro "github.com/brotherlogic/recordsorganiser/proto"
)

func InitTestServer() *Server {
	s := Init()
	s.SkipLog = true
	s.GoServer.KSclient = *keystoreclient.GetTestClient(".test")
	s.GoServer.KSclient.Save(context.Background(), KEY, &pb.State{})
	s.rGetter = &testGetter{}
	s.org = &testOrg{}
	return s
}

type testOrg struct {
	locations []*pbro.Location
	fail      bool
}

func (p *testOrg) getLocations(ctx context.Context) ([]*pbro.Location, error) {
	if p.fail {
		return []*pbro.Location{}, fmt.Errorf("Built to fail")
	}
	return p.locations, nil
}

type testUpdater struct {
	lastScore int32
	fail      bool
}

func (t *testUpdater) update(ctx context.Context, config *pb.State, id, rating int32) error {
	if t.fail {
		return fmt.Errorf("Build to fail")
	}
	t.lastScore = rating
	return nil
}

func (t *testUpdater) audition(ctx context.Context, id, rating int32) error {
	return nil
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

	rec, err := s.getCategoryRecord(context.Background(), time.Now(), pbrc.ReleaseMetadata_PRE_FRESHMAN, &pb.State{})
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

	rec, err := s.getCategoryRecord(context.Background(), time.Now(), pbrc.ReleaseMetadata_PRE_FRESHMAN, &pb.State{})
	if err == nil {
		t.Errorf("Did not fail: %v", rec)
	}
}

func TestCategoryEmpty(t *testing.T) {
	s := InitTestServer()

	rec, err := s.getCategoryRecord(context.Background(), time.Now(), pbrc.ReleaseMetadata_PRE_FRESHMAN, &pb.State{})
	if err != nil || rec != nil {
		t.Errorf("Did not fail: %v -> %v", rec, err)
	}
}

func TestGetInFolderFailOnCategoryGet(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{failGetInFolder: true}

	rec, err := s.getInFolderWithCategory(context.Background(), time.Now(), int32(12), pbrc.ReleaseMetadata_PRE_FRESHMAN, &pb.State{}, false, true)

	if err == nil {
		t.Errorf("Did not fail: %v", rec)
	}
}

func TestGetInFolderWithCategoryEmpty(t *testing.T) {
	s := InitTestServer()

	rec, err := s.getInFolderWithCategory(context.Background(), time.Now(), int32(12), pbrc.ReleaseMetadata_PRE_FRESHMAN, &pb.State{}, false, true)

	if err != nil || rec != nil {
		t.Errorf("Did not fail: %v -> %v", rec, err)
	}
}

func TestGetInFolder(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{CdPath: "blah", Category: pbrc.ReleaseMetadata_PRE_FRESHMAN}, Release: &pbgd.Release{InstanceId: 1}}}}

	rec, err := s.getInFolders(context.Background(), time.Now(), []int32{12}, &pb.State{}, false)
	if err != nil {
		t.Errorf("Did not fail: %v", err)
	}

	if rec == nil {
		t.Errorf("No record returned")
	}
}

func TestGetInFolderEmpty(t *testing.T) {
	s := InitTestServer()

	rec, err := s.getInFolders(context.Background(), time.Now(), []int32{12}, &pb.State{}, false)
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

	rec, err := s.getInFolders(context.Background(), time.Now(), []int32{12}, &pb.State{}, false)
	if err == nil {
		t.Errorf("Did not fail: %v", rec)
	}

}

func TestAddFolders(t *testing.T) {
	s := InitTestServer()
	s.org = &testOrg{locations: []*pbro.Location{&pbro.Location{Name: "blah1", InPlay: pbro.Location_IN_PLAY, FolderIds: []int32{12, 13}}, &pbro.Location{Name: "blah2", InPlay: pbro.Location_NOT_IN_PLAY, FolderIds: []int32{14, 15}}}}

	err := s.readLocations(context.Background(), &pb.State{})
	if err != nil {
		t.Errorf("Failure in reading: %v", err)
	}

}

func TestAddFoldersFail(t *testing.T) {
	s := InitTestServer()
	s.org = &testOrg{fail: true}

	err := s.readLocations(context.Background(), &pb.State{})

	if err == nil {
		t.Errorf("Location read did not fail")
	}
}

func TestRemoveSeven(t *testing.T) {
	s := InitTestServer()

	res := s.removeSeven([]int32{267116, 12}, &pb.State{})

	if len(res) != 1 {
		t.Errorf("Bad remove: %v", res)
	}
}

func TestValidate(t *testing.T) {
	s := InitTestServer()

	valid := s.validate(&pbrc.Record{Release: &pbgd.Release{Formats: []*pbgd.Format{&pbgd.Format{Descriptions: []string{"7\""}}}}}, &pb.State{})
	if !valid {
		t.Errorf("Baseline should be valid")
	}
}

func TestInvalid(t *testing.T) {
	s := InitTestServer()
	ti := time.Now()

	for i := 0; i < 10; i++ {
		s.countSeven(ti, &pb.State{})
	}
}
