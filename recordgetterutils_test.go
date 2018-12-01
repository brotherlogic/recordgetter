package main

import (
	"fmt"
	"testing"

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
	s.cdproc = &testRipper{}
	return s
}

type testRipper struct {
	ripped bool
}

func (t *testRipper) isRipped(ID int32) bool {
	return t.ripped
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
	nr := s.needsRip(&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, Formats: []*pbgd.Format{&pbgd.Format{Name: "CD"}}}})

	if !nr {
		t.Errorf("Should be reported as nedding a riop")
	}
}

func TestNotNeedsRip(t *testing.T) {
	s := InitTestServer()
	s.cdproc = &testRipper{ripped: true}
	nr := s.needsRip(&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, Formats: []*pbgd.Format{&pbgd.Format{Name: "CD"}}}})

	if nr {
		t.Errorf("Should be reported as nedding a riop")
	}
}

func TestNumberListens(t *testing.T) {
	if getNumListens(&pbrc.Record{Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN}}) != 3 {
		t.Errorf("Bad number of listens")
	}
}
