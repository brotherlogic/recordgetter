package main

import (
	"strings"
	"time"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pb "github.com/brotherlogic/recordgetter/proto"
)

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
