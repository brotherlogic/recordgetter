package main

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
	pbt "github.com/brotherlogic/tracer/proto"
)

//GetRecord gets a record
func (s *Server) GetRecord(ctx context.Context, in *pb.GetRecordRequest) (*pb.GetRecordResponse, error) {
	ctx = s.LogTrace(ctx, "GetRecord", time.Now(), pbt.Milestone_START_FUNCTION)
	if s.state.CurrentPick != nil {
		if in.GetRefresh() {
			rec, err := s.rGetter.getRelease(ctx, s.state.CurrentPick.Release.InstanceId)
			if err == nil && len(rec.GetRecords()) == 1 {
				s.state.CurrentPick = rec.GetRecords()[0]
			}
		}
		disk := int32(1)
		for _, score := range s.state.Scores {
			if score.InstanceId == s.state.CurrentPick.GetRelease().InstanceId {
				if score.DiskNumber >= disk {
					disk = score.DiskNumber + 1
				}
			}
		}

		s.LogTrace(ctx, "GetRecord", time.Now(), pbt.Milestone_END_FUNCTION)
		return &pb.GetRecordResponse{Record: s.state.CurrentPick, NumListens: getNumListens(s.state.CurrentPick), Disk: disk}, nil
	}

	rec, err := s.getReleaseFromPile(ctx, time.Now())
	if err != nil {
		return nil, err
	}

	if rec == nil {
		return nil, nil
	}

	disk := int32(1)
	s.LogTrace(ctx, fmt.Sprintf("Start Score Search (%v)", len(s.state.Scores)), time.Now(), pbt.Milestone_MARKER)
	if s.state.Scores != nil {
		for _, score := range s.state.Scores {
			if score.InstanceId == rec.GetRelease().InstanceId {
				if score.DiskNumber >= disk {
					disk = score.DiskNumber + 1
				}
			}
		}
	}

	s.LogTrace(ctx, "End Score Search", time.Now(), pbt.Milestone_MARKER)

	s.state.CurrentPick = rec
	s.saveState(ctx)

	s.LogTrace(ctx, "GetRecord", time.Now(), pbt.Milestone_END_FUNCTION)
	return &pb.GetRecordResponse{Record: rec, NumListens: getNumListens(rec), Disk: disk}, nil
}

//Listened marks a record as Listened
func (s *Server) Listened(ctx context.Context, in *pbrc.Record) (*pb.Empty, error) {
	ctx = s.LogTrace(ctx, "Listened", time.Now(), pbt.Milestone_START_FUNCTION)
	score := s.getScore(in)
	if score >= 0 {
		in.Release.Rating = score
		s.updater.update(ctx, in)
	}
	s.state.CurrentPick = nil
	s.saveState(ctx)
	s.LogTrace(ctx, "Listened", time.Now(), pbt.Milestone_END_FUNCTION)
	return &pb.Empty{}, nil
}

//Force forces a repick
func (s *Server) Force(ctx context.Context, in *pb.Empty) (*pb.Empty, error) {
	s.state.CurrentPick = nil
	s.saveState(ctx)
	return &pb.Empty{}, nil
}
