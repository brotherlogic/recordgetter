package main

import (
	"fmt"
	"strings"
	"time"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
	"golang.org/x/net/context"
)

func (s *Server) getPreFreshman(ctx context.Context, t time.Time) (*pbrc.Record, error) {
	pDate := int64(0)
	var newRec *pbrc.Record
	newRec = nil

	if t.Sub(s.lastPre) > time.Hour*3 {
		recs, err := s.rGetter.getRecordsInCategory(ctx, pbrc.ReleaseMetadata_PRE_FRESHMAN)
		if err != nil {
			return nil, err
		}

		for _, id := range recs {
			rc, err := s.rGetter.getRelease(ctx, id)
			if err == nil {
				if (pDate == 0 || rc.GetMetadata().DateAdded < pDate) && rc.GetRelease().Rating == 0 && !rc.GetMetadata().GetDirty() {
					if s.dateFine(rc, t) && !s.needsRip(rc) {
						pDate = rc.GetMetadata().DateAdded
						newRec = rc
					}
				}
			}
		}

		if newRec != nil {
			s.lastPre = time.Now()
			return newRec, nil
		}
	}

	return nil, nil
}

func (s *Server) getStagedToSell(ctx context.Context, t time.Time) (*pbrc.Record, error) {
	recs, err := s.rGetter.getRecordsInCategory(ctx, pbrc.ReleaseMetadata_STAGED_TO_SELL)
	if err != nil {
		return nil, err
	}

	for _, id := range recs {
		rc, err := s.rGetter.getRelease(ctx, id)
		if err == nil {
			if rc.GetMetadata().SetRating == 0 && rc.GetRelease().Rating == 0 {
				s.Log(fmt.Sprintf("Checking %v -> %v and %v", rc.GetRelease().Id, s.dateFine(rc, t), s.needsRip(rc)))
				if s.dateFine(rc, t) && !s.needsRip(rc) {
					return rc, nil
				}
			}
		}
	}

	return nil, nil
}

func (s *Server) needsRip(r *pbrc.Record) bool {
	// Digital records don't need to be ripped
	if r.GetMetadata().GetGoalFolder() == 268147 || r.GetMetadata().GetGoalFolder() == 1433217 {
		return false
	}

	for _, f := range r.GetRelease().Formats {
		if strings.Contains(f.Name, "CD") {
			return len(r.GetMetadata().CdPath) == 0
		}
	}

	return false
}

func (s *Server) clearScores(instanceID int32) {
	i := 0
	for i < len(s.state.Scores) {
		if s.state.Scores[i].InstanceId == instanceID {
			s.state.Scores[i] = s.state.Scores[len(s.state.Scores)-1]
			s.state.Scores = s.state.Scores[:len(s.state.Scores)-1]
		} else {
			i++
		}

	}
}

func (s *Server) getScore(rc *pbrc.Record) int32 {
	sum := int32(0)
	count := int32(0)

	sum += rc.Release.Rating
	count++
	maxDisk := int32(1)

	for _, score := range s.state.Scores {
		if score.InstanceId == rc.Release.InstanceId {
			sum += score.Score
			count++

			if score.DiskNumber >= maxDisk {
				maxDisk = score.DiskNumber + 1
			}
		}
	}

	//Add the score
	s.state.Scores = append(s.state.Scores, &pb.DiskScore{InstanceId: rc.GetRelease().InstanceId, DiskNumber: maxDisk, ScoreDate: time.Now().Unix(), Score: rc.GetRelease().Rating})

	if count >= rc.Release.FormatQuantity {
		s.clearScores(rc.Release.InstanceId)
		//Trick Rounding
		return int32((float64(sum) / float64(count)) + 0.5)
	}

	return -1
}

func getNumListens(rc *pbrc.Record) int32 {
	if rc.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_PRE_FRESHMAN {
		return 3
	}
	return 1
}
