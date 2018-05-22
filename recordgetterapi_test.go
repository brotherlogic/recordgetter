package main

import (
	"fmt"
	"log"
	"testing"
	"time"

	"golang.org/x/net/context"

	pbgd "github.com/brotherlogic/godiscogs"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
)

type testGetter struct {
	records []*pbrc.Record
	fail    bool
}

func (tg *testGetter) getRecords() (*pbrc.GetRecordsResponse, error) {
	if tg.fail {
		return nil, fmt.Errorf("Built to Fail")
	}
	return &pbrc.GetRecordsResponse{Records: tg.records}, nil
}
func (tg *testGetter) getRelease(ctx context.Context, instanceID int32) (*pbrc.GetRecordsResponse, error) {
	return &pbrc.GetRecordsResponse{Records: tg.records}, nil
}

func TestScoreRecordDiff(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}},
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}

	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})
	if err != nil {
		t.Fatalf("Error getting record: %v", err)
	}

	val := resp.GetRecord().GetRelease().InstanceId

	resp.GetRecord().GetRelease().Rating = 4
	_, err = s.Listened(context.Background(), resp.GetRecord())
	if err != nil {
		t.Fatalf("Error marking listened!: %v", err)
	}

	resp2, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})
	if err != nil {
		t.Fatalf("Error getting record: %v", err)
	}

	log.Printf("Returned: %v", resp2)
	if resp2.GetRecord().GetRelease().InstanceId == val {
		t.Errorf("Same record back %v vs %v", resp, resp2)
	}
}

func TestRecordGetDiskReturn(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}

	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})

	if err != nil {
		t.Fatalf("Error getting record: %v", err)
	}

	if resp.Disk != 1 {
		t.Errorf("Disk was not reported: %v", resp)
	}
}

func TestRecordGetDiskSkipOnDate(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}},
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}
	s.state.Scores = append(s.state.Scores, &pb.DiskScore{InstanceId: 1234, DiskNumber: 1, ScoreDate: time.Now().Unix(), Score: 5})

	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})
	if err != nil {
		t.Fatalf("Error getting record: %v", err)
	}

	if resp.Record.GetRelease().InstanceId != 12 {
		t.Errorf("Wrong record returned: %v", resp)
	}
}

func TestRecordGetNextDisk(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}},
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}

	s.state.Scores = append(s.state.Scores, &pb.DiskScore{InstanceId: 1234, DiskNumber: 1, ScoreDate: time.Now().AddDate(0, -1, 0).Unix(), Score: 5})

	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})
	if err != nil {
		t.Fatalf("Error getting record: %v", err)
	}

	if resp.Record.GetRelease().InstanceId != 1234 {
		t.Errorf("Wrong record returned: %v", resp)
	}

	if resp.Disk != 2 {
		t.Errorf("Wrong disk number returned %v", resp)
	}
}

func TestGetDiskOnCurrent(t *testing.T) {
	s := InitTestServer()
	s.state.CurrentPick = &pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}}
	s.state.Scores = append(s.state.Scores, &pb.DiskScore{InstanceId: 1234, DiskNumber: 1, ScoreDate: time.Now().AddDate(0, -1, 0).Unix(), Score: 5})

	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})

	if err != nil {
		t.Errorf("Error forcing: %v", err)
	}

	if resp.Disk != 2 {
		t.Errorf("No disk on current pick: %v", resp)
	}
}

func TestForce(t *testing.T) {
	s := InitTestServer()
	s.state.CurrentPick = &pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}}

	_, err := s.Force(context.Background(), &pb.Empty{})

	if err != nil {
		t.Errorf("Error forcing: %v", err)
	}

	if s.state.CurrentPick != nil {
		t.Errorf("Pick has not been nil'd: %v", s.state.CurrentPick)
	}
}

func TestRecordGetFailGet(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{
		fail: true,
		records: []*pbrc.Record{
			&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}},
		},
	}

	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})
	if err == nil {
		t.Fatalf("No error on get: %v", resp)
	}
}

func TestRecordGetRefresh(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{
		records: []*pbrc.Record{
			&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}},
		},
	}
	s.state.CurrentPick = &pbrc.Record{Release: &pbgd.Release{InstanceId: 12}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}}

	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{Refresh: true})
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}

	if resp.GetRecord().GetRelease().FormatQuantity != 2 {
		t.Errorf("Record has not been refreshed: %v", resp)
	}
}
