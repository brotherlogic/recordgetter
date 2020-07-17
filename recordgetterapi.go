package main

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
)

//GetRecord gets a record
func (s *Server) GetRecord(ctx context.Context, in *pb.GetRecordRequest) (*pb.GetRecordResponse, error) {
	t1 := time.Now()
	defer func() {
		time.Sleep(time.Second * 2)
		s.Log(fmt.Sprintf("TOOK %v", time.Now().Sub(t1)))
	}()
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}

	s.Log(fmt.Sprintf("Loaded: %v and %v", state, err))
	s.requests++
	if state.CurrentPick != nil {
		if in.GetRefresh() {
			rec, err := s.rGetter.getRelease(ctx, state.CurrentPick.Release.InstanceId)
			if err == nil {
				state.CurrentPick = rec
			}
		}
		disk := int32(1)
		for _, score := range state.Scores {
			if score.InstanceId == state.CurrentPick.GetRelease().InstanceId {
				if score.DiskNumber >= disk {
					disk = score.DiskNumber + 1
				}
			}
		}

		return &pb.GetRecordResponse{Record: state.CurrentPick, NumListens: getNumListens(state.CurrentPick), Disk: disk}, nil
	}

	rec, err := s.getReleaseFromPile(ctx, state, time.Now())
	if err != nil {
		return nil, err
	}

	disk := int32(1)
	if rec != nil && state.Scores != nil {
		for _, score := range state.Scores {
			if score.InstanceId == rec.GetRelease().InstanceId {
				if score.DiskNumber >= disk {
					disk = score.DiskNumber + 1
				}
			}
		}
	}

	state.CurrentPick = rec

	return &pb.GetRecordResponse{Record: rec, NumListens: getNumListens(rec), Disk: disk}, s.saveState(ctx, state)
}

//Listened marks a record as Listened
func (s *Server) Listened(ctx context.Context, in *pbrc.Record) (*pb.Empty, error) {
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}

	score := s.getScore(in, state)
	if score >= 0 {
		err := s.updater.update(ctx, in.GetRelease().GetInstanceId(), score)
		if err != nil && status.Convert(err).Code() != codes.OutOfRange {
			return &pb.Empty{}, err
		}
	}

	state.CurrentPick = nil
	return &pb.Empty{}, s.saveState(ctx, state)
}

//Force forces a repick
func (s *Server) Force(ctx context.Context, in *pb.Empty) (*pb.Empty, error) {
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}
	state.CurrentPick = nil
	s.saveState(ctx, state)
	return &pb.Empty{}, nil
}
