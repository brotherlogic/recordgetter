package main

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brotherlogic/goserver/utils"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
	rwpb "github.com/brotherlogic/recordwants/proto"
)

// GetRecord gets a record
func (s *Server) GetRecord(ctx context.Context, in *pb.GetRecordRequest) (*pb.GetRecordResponse, error) {
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}
	s.CtxLog(ctx, fmt.Sprintf("SC: %v", state.ScoreCount))

	if in.GetType() == pb.RequestType_AUDITION {
		if state.AuditionPick > 0 {
			rec, err := s.rGetter.getRelease(ctx, state.AuditionPick)

			if err != nil {
				return nil, err
			}

			disk := int32(1)
			for _, score := range state.Scores {
				if score.InstanceId == state.AuditionPick {
					if score.DiskNumber >= disk {
						disk = score.DiskNumber + 1
					}
				}
			}

			return &pb.GetRecordResponse{Record: rec, Disk: disk}, nil
		}

		rec, err := s.rGetter.getAuditionRelease(ctx)
		if err != nil {
			return nil, err
		}
		state.AuditionPick = rec.GetRelease().GetInstanceId()

		disk := int32(1)
		for _, score := range state.Scores {
			if score.InstanceId == state.AuditionPick {
				if score.DiskNumber >= disk {
					disk = score.DiskNumber + 1
				}
			}
		}

		return &pb.GetRecordResponse{Record: rec, Disk: disk}, s.saveState(ctx, state)

	}

	s.requests++
	if in.GetType() == pb.RequestType_DIGITAL {
		if state.CurrentDigitalPick > 0 {
			rec, err := s.rGetter.getRelease(ctx, state.CurrentDigitalPick)

			if err != nil {
				return nil, err
			}
			disk := int32(1)
			for _, score := range state.Scores {
				if score.InstanceId == state.CurrentDigitalPick {
					if score.DiskNumber >= disk {
						disk = score.DiskNumber + 1
					}
				}
			}
			return &pb.GetRecordResponse{Record: rec,
				Disk: disk}, nil
		}
	} else if in.GetType() == pb.RequestType_DEFAULT {
		if state.CurrentPick != nil && state.CurrentPick.GetRelease().GetId() > 0 {
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
	}

	if in.GetType() == pb.RequestType_CD_FOCUS {
		if state.CurrentCdPick > 0 {
			rec, err := s.rGetter.getRelease(ctx, state.CurrentCdPick)

			if err != nil {
				if status.Code(err) == codes.OutOfRange {
					state.CurrentCdPick = 0
					return &pb.GetRecordResponse{}, s.saveState(ctx, state)
				}
				return nil, err
			}
			disk := int32(1)
			for _, score := range state.Scores {
				if score.InstanceId == state.CurrentCdPick {
					if score.DiskNumber >= disk {
						disk = score.DiskNumber + 1
					}
				}
			}
			return &pb.GetRecordResponse{Record: rec,
				Disk: disk}, nil
		}
	}

	key, err := s.RunLockingElection(ctx, "recordgetter", "Locking to pick record to get")
	if err != nil {
		return nil, err
	}
	defer s.ReleaseLockingElection(ctx, "recordgetter", key)

	rec, err := s.getReleaseFromPile(ctx, state, time.Now(), in.GetType())
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

	if in.GetType() == pb.RequestType_DIGITAL {
		state.CurrentDigitalPick = rec.Release.GetInstanceId()
	} else if in.GetType() == pb.RequestType_CD_FOCUS {
		state.CurrentCdPick = rec.GetRelease().GetInstanceId()
	} else {
		state.CurrentPick = rec
		state.CatCount[pbrc.ReleaseMetadata_Category_value[rec.GetMetadata().GetCategory().String()]]++
	}

	if rec.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_STAGED_TO_SELL && rec.GetMetadata().GetSaleAttempts() > 5 {
		s.RaiseIssue(fmt.Sprintf("Figure out sale %v", rec.GetRelease().GetTitle()), fmt.Sprintf("To sell or not to sell?: %v", rec.GetRelease().GetInstanceId()))
	}

	// If we're picking - also defer an update to the display; do so best effort
	f := func() {
		ctx, cancel := utils.ManualContext("getter-display-ping", time.Minute)
		defer cancel()
		c, err := s.FDialServer(ctx, "display")
		if err == nil {
			defer c.Close()
			cup := pbrc.NewClientUpdateServiceClient(c)
			cup.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: rec.GetRelease().GetInstanceId()})
		}
	}
	defer f()
	return &pb.GetRecordResponse{Record: rec, NumListens: getNumListens(rec), Disk: disk}, s.saveState(ctx, state)
}

// Listened marks a record as Listened
func (s *Server) Listened(ctx context.Context, in *pbrc.Record) (*pb.Empty, error) {
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}

	// We've listended - so trigger a new get
	f := func() {
		ctx, cancel := utils.ManualContext("getter-display-ping", time.Minute)
		defer cancel()
		c, err := s.FDialServer(ctx, "display")
		if err == nil {
			defer c.Close()
			cup := pbrc.NewClientUpdateServiceClient(c)
			cup.ClientUpdate(ctx, &pbrc.ClientUpdateRequest{InstanceId: in.GetRelease().GetInstanceId()})
		}
	}
	defer f()

	// This is a want rather than a record
	if in.GetRelease().GetInstanceId() == 0 {
		s.DLog(ctx, fmt.Sprintf("Tracking a want change: %v -> %v", in.GetRelease().GetId(), in))
		if in.GetMetadata().GetSetRating() == 5 {
			// Get the OG vinyl
			err = s.wants.updateWant(ctx, in.GetRelease().GetId(), rwpb.MasterWant_WANT_OG)
			if err != nil {
				return nil, err
			}
		} else if in.GetMetadata().GetSetRating() == 4 {
			// Get the digital version
			err = s.wants.updateWant(ctx, in.GetRelease().GetId(), rwpb.MasterWant_WANT_DIGITAL)
			if err != nil {
				return nil, err
			}
		} else {
			// Set to never
			err = s.wants.updateWant(ctx, in.GetRelease().GetId(), rwpb.MasterWant_NEVER)
			if err != nil {
				return nil, err
			}
		}
		state.LastWant = time.Now().Unix()

		state.CurrentDigitalPick = 0
		state.CurrentPick = nil

	} else if state.GetCurrentPick().GetRelease().GetInstanceId() == in.GetRelease().GetInstanceId() {
		record, err := s.rGetter.getRelease(ctx, in.GetRelease().GetInstanceId())
		if err != nil {
			return nil, err
		}

		score := s.getScore(ctx, in, state)
		// Immediate score on sale items
		if record.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_STAGED_TO_SELL {
			score = in.GetMetadata().GetSetRating()
		}

		if score >= 0 {
			err := s.updater.update(ctx, state, in.GetRelease().GetInstanceId(), score)
			if err != nil && status.Convert(err).Code() != codes.OutOfRange {
				return &pb.Empty{}, err
			}
		}
		state.CurrentPick = nil
	} else if state.GetCurrentDigitalPick() == in.GetRelease().GetInstanceId() {
		score := s.getScore(ctx, in, state)
		if score >= 0 {
			err := s.updater.update(ctx, state, in.GetRelease().GetInstanceId(), score)
			if err != nil && status.Convert(err).Code() != codes.OutOfRange {
				return &pb.Empty{}, err
			}
		}
		state.CurrentDigitalPick = 0
	} else if state.GetAuditionPick() == in.GetRelease().GetInstanceId() {
		score := s.getScore(ctx, in, state)
		if score >= 0 {
			s.DLog(ctx, fmt.Sprintf("AUDITIONING with %v", score))
			err := s.updater.audition(ctx, in.GetRelease().GetInstanceId(), score)
			if err != nil && status.Convert(err).Code() != codes.OutOfRange {
				return &pb.Empty{}, err
			}
			state.AuditionPick = 0
		}
	} else if state.GetCurrentCdPick() == in.GetRelease().GetInstanceId() {
		score := s.getScore(ctx, in, state)
		if score >= 0 {
			err := s.updater.update(ctx, state, in.GetRelease().GetInstanceId(), score)
			if err != nil && status.Convert(err).Code() != codes.OutOfRange {
				return &pb.Empty{}, err
			}
		}
		state.CurrentCdPick = 0
	}

	return &pb.Empty{}, s.saveState(ctx, state)
}

// Force forces a repick
func (s *Server) Force(ctx context.Context, in *pb.ForceRequest) (*pb.Empty, error) {
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}
	switch in.GetType() {
	case pb.RequestType_AUDITION:
		state.AuditionPick = -1
	case pb.RequestType_DIGITAL:
		state.CurrentDigitalPick = -1
	}
	state.CurrentPick = nil
	s.saveState(ctx, state)
	return &pb.Empty{}, nil
}
