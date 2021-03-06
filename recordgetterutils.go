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

func (s *Server) countSeven(t time.Time, state *pb.State) bool {
	if t.YearDay() == int(state.GetSevenDay()) {
		state.SevenCount++
		return state.SevenCount <= 10
	}

	state.SevenDay = int32(t.YearDay())
	state.SevenCount = 1
	return true
}

func (s *Server) validate(rec *pbrc.Record, state *pb.State) bool {
	for _, format := range rec.GetRelease().GetFormats() {
		for _, form := range format.GetDescriptions() {
			if strings.Contains(form, "7") {
				return s.countSeven(time.Now(), state)
			}
		}
	}

	return true
}

func (s *Server) getCategoryRecord(ctx context.Context, t time.Time, c pbrc.ReleaseMetadata_Category, state *pb.State) (*pbrc.Record, error) {
	pDate := int64(0)
	var newRec *pbrc.Record
	newRec = nil

	recs, err := s.rGetter.getRecordsInCategory(ctx, c)

	if err != nil {
		return nil, err
	}

	for _, id := range recs {
		rc, err := s.rGetter.getRelease(ctx, id)
		if err == nil && rc != nil {
			if (pDate == 0 || rc.GetMetadata().DateAdded < pDate) && rc.GetRelease().Rating == 0 && !rc.GetMetadata().GetDirty() && rc.GetMetadata().SetRating == 0 {
				if s.dateFine(rc, t, state) && !s.needsRip(rc) {
					pDate = rc.GetMetadata().DateAdded
					newRec = rc
				}
			}
		}
	}

	if newRec != nil {
		return newRec, nil
	}

	return nil, nil
}

func (s *Server) getInFolderWithCategory(ctx context.Context, t time.Time, folder int32, cat pbrc.ReleaseMetadata_Category, state *pb.State) (*pbrc.Record, error) {
	recs, err := s.rGetter.getRecordsInFolder(ctx, folder)
	if err != nil {
		return nil, err
	}

	for _, id := range recs {
		r, err := s.rGetter.getRelease(ctx, id)
		if err == nil {
			if r.GetMetadata().GetCategory() == cat && r.GetRelease().Rating == 0 && !r.GetMetadata().GetDirty() && r.GetMetadata().SetRating == 0 {
				if s.dateFine(r, t, state) && !s.needsRip(r) {
					return r, nil
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

func (s *Server) getInFolders(ctx context.Context, t time.Time, folders []int32, state *pb.State) (*pbrc.Record, error) {
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

func (s *Server) getScore(rc *pbrc.Record, state *pb.State) int32 {
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
		s.Log(fmt.Sprintf("Scoring %v, from %v (%v)", score, scores, rc.GetMetadata().GetSetRating()))
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
