package main

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	pbgd "github.com/brotherlogic/godiscogs"
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
func (tg *testGetter) getRelease(ctx context.Context, instanceID int32) (*pbrc.Record, error) {
	if len(tg.records) > 0 {
		return tg.records[0], nil
	}
	return nil, nil
}

func (tg *testGetter) getRecordsInCategory(ctx context.Context, category pbrc.ReleaseMetadata_Category) ([]int32, error) {
	if tg.failGetInCategory {
		return []int32{}, fmt.Errorf("Built to fail")
	}
	return []int32{1}, nil
}

func (tg *testGetter) getRecordsInFolder(ctx context.Context, folder int32) ([]int32, error) {
	if tg.failGetInFolder {
		return []int32{}, fmt.Errorf("Built to fail")
	}
	return []int32{1}, nil
}

func TestGetFromDigital(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 1}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_SOPHMORE, DateAdded: 12}},
	}}
}

func TestScoreFailGet(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 1}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_SOPHMORE, DateAdded: 12}},
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 1}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}

	s.GetRecord(context.Background(), &pb.GetRecordRequest{})
}

func TestScoreRecordGadPull(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_FRESHMAN, DateAdded: 12}},
	}}

	s.GetRecord(context.Background(), &pb.GetRecordRequest{})
}

func TestRecordGetDiskReturn(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}

	s.GetRecord(context.Background(), &pb.GetRecordRequest{})
}

func TestRecordGetDiskSkipOnDate(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_SOPHMORE, DateAdded: 12}},
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}

	s.GetRecord(context.Background(), &pb.GetRecordRequest{})
}

func TestRecordGetNextDisk(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{records: []*pbrc.Record{
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_SOPHMORE, DateAdded: 12}},
		&pbrc.Record{Release: &pbgd.Release{InstanceId: 1234, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_PROFESSOR, DateAdded: 1234}},
	}}

	s.GetRecord(context.Background(), &pb.GetRecordRequest{})
}

func TestForce(t *testing.T) {
	s := InitTestServer()

	_, err := s.Force(context.Background(), &pb.Empty{})

	if err != nil {
		t.Errorf("Error forcing: %v", err)
	}

}

func TestRecordGetRefresh(t *testing.T) {
	s := InitTestServer()
	s.rGetter = &testGetter{
		records: []*pbrc.Record{
			&pbrc.Record{Release: &pbgd.Release{InstanceId: 12, FormatQuantity: 2}, Metadata: &pbrc.ReleaseMetadata{Category: pbrc.ReleaseMetadata_PRE_SOPHMORE, DateAdded: 12}},
		},
	}

	s.GetRecord(context.Background(), &pb.GetRecordRequest{Refresh: true})
}

func TestGetRecord(t *testing.T) {
	s := InitTestServer()
	resp, err := s.GetRecord(context.Background(), &pb.GetRecordRequest{})
	if err == nil {
		t.Errorf("Empty get did not fail: %v", resp)
	}
}
