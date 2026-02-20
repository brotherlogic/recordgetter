package main

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brotherlogic/goserver/utils"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbr "github.com/brotherlogic/recorder/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
	rwpb "github.com/brotherlogic/recordwants/proto"
)

func (s *Server) ClientUpdate(ctx context.Context, req *pbrc.ClientUpdateRequest) (*pbrc.ClientUpdateResponse, error) {
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}

	s.CtxLog(ctx, fmt.Sprintf("Comparing %v with %v", state.GetCurrentPick().GetRelease().GetInstanceId(), req.GetInstanceId()))
	if state.GetCurrentPick().GetRelease().GetInstanceId() == req.GetInstanceId() {
		rec, err := s.rGetter.getRelease(ctx, req.GetInstanceId())
		if err != nil {
			return nil, err
		}
		s.CtxLog(ctx, fmt.Sprintf("Found rating %v", rec.GetRelease().GetRating()))
		if rec.GetRelease().GetRating() > 0 {
			state.CurrentPick = nil
		}

		return &pbrc.ClientUpdateResponse{}, s.saveState(ctx, state)
	}

	return &pbrc.ClientUpdateResponse{}, nil
}

func (s *Server) Clear(ctx context.Context, req *pb.ClearRequest) (*pb.ClearResponse, error) {
	config, err := s.loadState(ctx)
	if err != nil {
		return nil, err
	}

	var ns []*pb.DiskScore
	for _, score := range config.GetScores() {
		if score.GetInstanceId() != req.GetIid() {
			ns = append(ns, score)
		}
	}
	config.Scores = ns

	return &pb.ClearResponse{}, s.saveState(ctx, config)
}

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
	lkey := fmt.Sprintf("recordgetter-%v", in.GetType())
	key, err := s.RunLockingElection(ctx, lkey, "Locking to pick record to get")
	if err != nil {
		return nil, err
	}
	defer func() {
		t := time.Now()
		ctx, cancel := utils.ManualContext(fmt.Sprintf("recordgetter_cli-%v", os.Args[1]), time.Minute*5)
		defer cancel()
		err := fmt.Errorf("Bad error")
		for err != nil && time.Since(t) < time.Minute*5 {
			err = s.ReleaseLockingElection(ctx, lkey, key)
		}
	}()

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
		state.CattypeCount[fmt.Sprintf("%v%v", rec.GetMetadata().GetCategory(), rec.GetMetadata().GetFiledUnder())]++
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

	f2 := func() {
		ctx, cancel := utils.ManualContext("recorder-ping", time.Minute)
		defer cancel()
		c, err := s.FDial("recorder:8080")
		if err == nil {
			defer c.Close()
			rc := pbr.NewRecordGetterClient(c)
			_, err = rc.NewRecord(ctx, &pbr.NewRecordRequest{})
			s.CtxLog(ctx, fmt.Sprintf("Error: %v", err))
		}
	}
	defer f2()

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

		s.CtxLog(ctx, fmt.Sprintf("Scoring %v %v %v", score, record.GetMetadata().GetFiledUnder(), record.GetMetadata().GetCategory()))
		if score <= 3 && score > 0 &&
			record.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_12_INCH &&
			record.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_STAGED_TO_SELL {
			state.Sales++
		}

		if record.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL &&
			record.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_12_INCH {
			if time.Since(time.Unix(record.GetMetadata().DateArrived, 0)) < time.Hour*24*30*2 {
				s.CtxLog(ctx, fmt.Sprintf("Updating %v vs %v", time.Since(time.Unix(record.Metadata.GetDateArrived(), 0)), time.Hour*24*30*2))
				state.TwelvePhs++
			}
		}

		if record.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_PRE_IN_COLLECTION &&
			record.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_12_INCH {
			s.CtxLog(ctx, fmt.Sprintf("Updating %v vs %v", time.Since(time.Unix(record.Metadata.GetDateArrived(), 0)), time.Hour*24*30*2))
			state.TwlevePic++

		}

		// Immediate score on digital records
		if record.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_DIGITAL {
			score = in.GetMetadata().GetSetRating()
		}

		if score >= 0 {
			err := s.updater.update(ctx, state, in.GetRelease().GetInstanceId(), score)
			if err != nil && status.Convert(err).Code() != codes.OutOfRange {
				return &pb.Empty{}, err
			}
			if record.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_7_INCH && record.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_PRE_IN_COLLECTION {
				s.RaiseIssue("Add a 7 Inch Record", "You've just put one in the collection")
			}
		}
		state.CurrentPick = nil
	} else if state.GetCurrentDigitalPick() == in.GetRelease().GetInstanceId() {
		score := in.GetMetadata().GetSetRating()

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
