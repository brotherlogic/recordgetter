package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"golang.org/x/net/context"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
	pbro "github.com/brotherlogic/recordsorganiser/proto"
)

func (s *Server) validate(rec *pbrc.Record, typ pb.RequestType) bool {
	if rec.GetMetadata().GetDateArrived() == 0 {
		return false
	}

	// Check the type
	if typ == pb.RequestType_CD_FOCUS && rec.GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_CD {
		//if rec.GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_TAPE {
		return false
		//}
	}

	if typ == pb.RequestType_DIGITAL && rec.GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_DIGITAL {
		return false
	}

	// Records should be in the listening pile
	if s.visitors {
		return (rec.GetRelease().GetFolderId() == 812802 || rec.GetRelease().GetFolderId() == 7651472 || rec.GetRelease().GetFolderId() == 7665013 || rec.GetRelease().GetFolderId() == 7664293) &&
			(rec.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_CD ||
				rec.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_DIGITAL)
	} else {
		return rec.GetRelease().GetFolderId() == 812802 || rec.GetRelease().GetFolderId() == 7651472
	}
}

func (s *Server) isFilable(rc *pbrc.Record) bool {
	return rc.GetMetadata().GetGoalFolder() == 242017 && rc.GetRelease().GetFormatQuantity() <= 3
}

func (s *Server) getCategoryRecord(ctx context.Context, t time.Time, c pbrc.ReleaseMetadata_Category, state *pb.State, typ pb.RequestType) (*pbrc.Record, error) {
	pDate := int64(0)
	var newRec *pbrc.Record
	newRec = nil

	recs, err := s.rGetter.getRecordsInCategory(ctx, c)

	if err != nil {
		return nil, err
	}

	for _, id := range recs {
		rc, err := s.rGetter.getRelease(ctx, id)
		isDigital := rc.GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_12_INCH &&
			rc.GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_7_INCH

		// Include CDs in sale determinations at home
		if c == pbrc.ReleaseMetadata_STAGED_TO_SELL {
			isDigital = isDigital && rc.GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_CD
		}
		s.CtxLog(ctx, fmt.Sprintf("SKIP %v-> %v and %v", id, typ, rc.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_DIGITAL))
		if ((typ == pb.RequestType_CD_FOCUS || typ == pb.RequestType_DIGITAL) && !isDigital) ||
			(!(typ == pb.RequestType_CD_FOCUS || typ == pb.RequestType_DIGITAL) && isDigital) {
			continue
		}

		if !s.validate(rc, typ) {
			s.CtxLog(ctx, fmt.Sprintf("SKIP %v -> does not validate", id))
			continue
		}

		s.CtxLog(ctx, fmt.Sprintf("Evaluating %v from %v", rc.GetRelease().GetTitle(), c))
		if err == nil && rc != nil {
			if (pDate == 0 || rc.GetMetadata().DateAdded < pDate) &&
				rc.GetRelease().Rating == 0 &&
				!rc.GetMetadata().GetDirty() &&
				rc.GetMetadata().SetRating == 0 {
				if s.dateFine(rc, t, state) && !s.needsRip(rc) {
					pDate = rc.GetMetadata().DateAdded
					newRec = rc
				} else {
					s.CtxLog(ctx, fmt.Sprintf("Date is not fine for %v", id))
				}
			} else {
				s.CtxLog(ctx, fmt.Sprintf("Did not pass go %v -> %v: %v (%v), %v, %v", id, pDate, newRec.GetRelease().GetInstanceId(), rc.GetRelease().GetRating(), rc.GetMetadata().GetDirty(), rc.GetMetadata().GetSetRating()))
			}
		}
	}

	if newRec != nil {
		return newRec, nil
	}

	return nil, nil
}

func (s *Server) getInFolderWithCategory(ctx context.Context, t time.Time, folder int32, cat pbrc.ReleaseMetadata_Category, state *pb.State, digitalOnly bool, filable bool) (*pbrc.Record, error) {
	recs, err := s.rGetter.getRecordsInFolder(ctx, folder)
	if err != nil {
		return nil, err
	}

	for _, id := range recs {
		r, err := s.rGetter.getRelease(ctx, id)
		if (digitalOnly && r.GetMetadata().GetFiledUnder() != pbrc.ReleaseMetadata_FILE_DIGITAL) ||
			(!digitalOnly && r.GetMetadata().GetFiledUnder() == pbrc.ReleaseMetadata_FILE_DIGITAL) {
			continue
		}
		if err == nil {
			if r.GetMetadata().GetCategory() == cat && r.GetRelease().Rating == 0 && !r.GetMetadata().GetDirty() && r.GetMetadata().SetRating == 0 {
				if s.dateFine(r, t, state) && !s.needsRip(r) {
					if !filable || s.isFilable(r) {
						return r, nil
					}
				}
			}
		}
	}

	return nil, nil
}

func (s *Server) removeSeven(allrecs []int32, state *pb.State) []int32 {
	new := []int32{}
	for _, val := range allrecs {
		if time.Now().Sub(time.Unix(state.LastSeven, 0)) < time.Hour*2 || val != 267116 {
			new = append(new, val)
		}
	}
	return new
}

func (s *Server) setTime(r *pbrc.Record, state *pb.State) {
	if r.GetRelease().FolderId == 267116 {
		state.LastSeven = time.Now().Unix()
	}
}

func (s *Server) getInFolders(ctx context.Context, t time.Time, folders []int32, state *pb.State, dig bool) (*pbrc.Record, error) {
	allrecs := make([]int32, 0)
	for _, folder := range folders {
		recs, err := s.rGetter.getRecordsInFolder(ctx, folder)
		if err != nil {
			return nil, err
		}
		allrecs = append(allrecs, recs...)
	}

	allrecs = s.removeSeven(allrecs, state)

	// Shuffle allrecs to prevent bias
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allrecs), func(i, j int) { allrecs[i], allrecs[j] = allrecs[j], allrecs[i] })

	for _, id := range allrecs {
		r, err := s.rGetter.getRelease(ctx, id)
		if err == nil && r != nil {
			if r.GetRelease().Rating == 0 && !r.GetMetadata().GetDirty() && r.GetMetadata().SetRating == 0 {
				if s.dateFine(r, t, state) && !s.needsRip(r) {
					s.setTime(r, state)
					return r, nil
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

func (s *Server) clearScores(instanceID int32, state *pb.State) {
	i := 0
	for i < len(state.Scores) {
		if state.Scores[i].InstanceId == instanceID {
			state.Scores[i] = state.Scores[len(state.Scores)-1]
			state.Scores = state.Scores[:len(state.Scores)-1]
		} else {
			i++
		}

	}
}

func (s *Server) getScore(ctx context.Context, rc *pbrc.Record, state *pb.State) int32 {
	sum := int32(0)
	count := int32(0)

	sum += rc.GetMetadata().GetSetRating()
	count++
	maxDisk := int32(1)

	scores := ""
	for _, score := range state.Scores {
		if score.InstanceId == rc.Release.InstanceId {
			scores += fmt.Sprintf(" %v ", score)
			sum += score.Score
			count++

			if score.DiskNumber >= maxDisk {
				maxDisk = score.DiskNumber + 1
			}
		}
	}

	//Add the score
	state.Scores = append(state.Scores, &pb.DiskScore{InstanceId: rc.GetRelease().InstanceId, DiskNumber: maxDisk, ScoreDate: time.Now().Unix(), Score: rc.GetMetadata().GetSetRating()})

	if count >= rc.Release.FormatQuantity {
		s.clearScores(rc.Release.InstanceId, state)

		//Trick Rounding
		score := int32((float64(sum) / float64(count)) + 0.5)
		return score
	}

	return -1
}

func getNumListens(rc *pbrc.Record) int32 {
	if rc.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_PRE_FRESHMAN {
		return 3
	}
	return 1
}

func (s *Server) readLocations(ctx context.Context, state *pb.State) error {
	locations, err := s.org.getLocations(ctx)
	if err != nil {
		return err
	}

	state.ActiveFolders = []int32{}
	for _, location := range locations {
		if location.InPlay == pbro.Location_IN_PLAY {
			for _, folder := range location.FolderIds {
				state.ActiveFolders = append(state.ActiveFolders, folder)
			}
		}
	}

	return s.saveState(ctx, state)
}
