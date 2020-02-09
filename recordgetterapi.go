package main

import (
	"time"

	"golang.org/x/net/context"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
)

//GetRecord gets a record
func (s *Server) GetRecord(ctx context.Context, in *pb.GetRecordRequest) (*pb.GetRecordResponse, error) {
	s.requests++
	if s.state.CurrentPick != nil {
		if in.GetRefresh() {
			rec, err := s.rGetter.getRelease(ctx, s.state.CurrentPick.Release.InstanceId)
			if err == nil {
				s.state.CurrentPick = rec
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

		return &pb.GetRecordResponse{Record: s.state.CurrentPick, NumListens: getNumListens(s.state.CurrentPick), Disk: disk}, nil
	}

	rec, err := s.getReleaseFromPile(ctx, time.Now())
	if err != nil {
		return nil, err
	}

	disk := int32(1)
	if rec != nil && s.state.Scores != nil {
		for _, score := range s.state.Scores {
			if score.InstanceId == rec.GetRelease().InstanceId {
				if score.DiskNumber >= disk {
					disk = score.DiskNumber + 1
				}
			}
		}
	}

	s.state.CurrentPick = rec

	return &pb.GetRecordResponse{Record: rec, NumListens: getNumListens(rec), Disk: disk}, s.saveState(ctx)
}

//Listened marks a record as Listened
func (s *Server) Listened(ctx context.Context, in *pbrc.Record) (*pb.Empty, error) {
	score := s.getScore(in)
	if score >= 0 {
		in.Release.Rating = score
		err := s.updater.update(ctx, in)
		if err != nil {
			return &pb.Empty{}, err
		}
	}
	s.state.CurrentPick = nil
	return &pb.Empty{}, s.saveState(ctx)
}

//Force forces a repick
func (s *Server) Force(ctx context.Context, in *pb.Empty) (*pb.Empty, error) {
	s.state.CurrentPick = nil
	s.saveState(ctx)
	return &pb.Empty{}, nil
}
