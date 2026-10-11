package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brotherlogic/goserver/utils"

	pbgd "github.com/brotherlogic/godiscogs/proto"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbrg "github.com/brotherlogic/recordgetter/proto"
	pbscores "github.com/brotherlogic/recordscores/proto"
	"google.golang.org/grpc"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
)

var stdout io.Writer = os.Stdout

type rgClient interface {
	GetRecord(ctx context.Context, in *pbrg.GetRecordRequest, opts ...grpc.CallOption) (*pbrg.GetRecordResponse, error)
}

var dialRecordGetter = func(ctx context.Context) (rgClient, func(), error) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		return nil, nil, err
	}
	return pbrg.NewRecordGetterClient(conn), func() { conn.Close() }, nil
}

type rcClient interface {
	QueryRecords(ctx context.Context, in *pbrc.QueryRecordsRequest, opts ...grpc.CallOption) (*pbrc.QueryRecordsResponse, error)
}

var dialRecordCollection = func(ctx context.Context) (rcClient, func(), error) {
	conn, err := utils.LFDialServer(ctx, "recordcollection")
	if err != nil {
		return nil, nil, err
	}
	return pbrc.NewRecordCollectionServiceClient(conn), func() { conn.Close() }, nil
}

func fetchDigitalVersionsStatus(ctx context.Context, release *pbgd.Release) string {
	if release == nil || len(release.GetDigitalVersions()) == 0 {
		return "Digital Versions: None available"
	}

	client, closer, err := dialRecordCollection(ctx)
	if err != nil {
		return "Digital Versions: [Unavailable - error querying recordcollection]"
	}
	defer closer()

	var owned []int32
	for _, dv := range release.GetDigitalVersions() {
		res, err := client.QueryRecords(ctx, &pbrc.QueryRecordsRequest{
			Query: &pbrc.QueryRecordsRequest_ReleaseId{ReleaseId: dv},
		})
		if err != nil {
			return "Digital Versions: [Unavailable - error querying recordcollection]"
		}
		if len(res.GetInstanceIds()) > 0 {
			owned = append(owned, dv)
		}
	}

	total := len(release.GetDigitalVersions())
	if len(owned) == 0 {
		return fmt.Sprintf("Digital Versions: None owned (0 of %d)", total)
	}

	var ownedStrs []string
	for _, id := range owned {
		ownedStrs = append(ownedStrs, fmt.Sprintf("%d", id))
	}
	return fmt.Sprintf("Digital Versions: Owned %d of %d (Owned: [%s])", len(owned), total, strings.Join(ownedStrs, ", "))
}

var dialRecordscores = func(ctx context.Context) (pbscores.RecordScoreServiceClient, func(), error) {
	conn, err := utils.LFDialServer(ctx, "recordscores")
	if err != nil {
		return nil, nil, err
	}
	return pbscores.NewRecordScoreServiceClient(conn), func() { conn.Close() }, nil
}

func fetchScoreHistoryStatus(ctx context.Context, instanceId int64) string {
	client, closer, err := dialRecordscores(ctx)
	if err != nil {
		return "Score History: [Unavailable - error querying recordscores]"
	}
	if closer != nil {
		defer closer()
	}

	resp, err := client.GetScore(ctx, &pbscores.GetScoreRequest{InstanceId: instanceId})
	if err != nil {
		return "Score History: [Unavailable - error querying recordscores]"
	}

	scores := resp.GetScores()
	if len(scores) == 0 {
		return "Score History: No history"
	}

	sorted := make([]*pbscores.Score, len(scores))
	copy(sorted, scores)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetScoreTime() < sorted[j].GetScoreTime()
	})

	var sum float64
	for _, s := range sorted {
		sum += float64(s.GetRating())
	}
	avg := sum / float64(len(sorted))

	start := 0
	if len(sorted) > 5 {
		start = len(sorted) - 5
	}
	lastScores := sorted[start:]
	ratingsStr := make([]string, len(lastScores))
	for i, s := range lastScores {
		ratingsStr[i] = fmt.Sprintf("%d", s.GetRating())
	}

	return fmt.Sprintf("Score History: %d listens [Ratings: %s] (Avg: %.1f)", len(scores), strings.Join(ratingsStr, ", "), avg)
}
func formatRipStatus(metadata *pbrc.ReleaseMetadata) string {
	if metadata == nil || metadata.GetLastRipDate() <= 0 {
		return "Ripped: Not ripped"
	}
	dateStr := time.Unix(metadata.GetLastRipDate(), 0).UTC().Format("2006-01-02")
	return fmt.Sprintf("Ripped: %s (Quality: %d/100)", dateStr, metadata.GetRippedQuality())
}

func formatSalePrice(metadata *pbrc.ReleaseMetadata) string {
	if metadata == nil {
		return "Sale Price: Not set"
	}
	price := metadata.GetCurrentSalePrice()
	if price <= 0 {
		price = metadata.GetSalePrice()
	}
	if price <= 0 {
		return "Sale Price: Not set"
	}
	dollars := price / 100
	cents := price % 100
	return fmt.Sprintf("Sale Price: $%d.%02d", dollars, cents)
}

func findServer(name string) (string, int) {
	ip, port, _ := utils.Resolve(name, "recordgetter-cli")
	return ip, int(port)
}

func clear(ctx context.Context, t pbrg.RequestType) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)
	r, err := client.Force(ctx, &pbrg.ForceRequest{Type: t})
	fmt.Printf("%v and %v", r, err)
}

func clean(ctx context.Context, iid int64) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()

	client := pbrg.NewRecordGetterClient(conn)
	r, err := client.Clear(ctx, &pbrg.ClearRequest{Iid: iid})
	fmt.Printf("%v and %v", r, err)
}

func cd(ctx context.Context) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Type: pbrg.RequestType_CD_FOCUS})
	if err != nil {
		log.Fatalf("Error on get: %v", err)
	}
	if len(r.GetRecord().GetRelease().GetArtists()) > 0 {
		fmt.Printf("%v - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetArtists()[0].GetName(),
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	} else {
		fmt.Printf("UnknownArtist - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	}
}

func digital(ctx context.Context) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Type: pbrg.RequestType_DIGITAL})
	if err != nil {
		log.Fatalf("Error on get: %v", err)
	}
	if len(r.GetRecord().GetRelease().GetArtists()) > 0 {
		fmt.Printf("%v - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetArtists()[0].GetName(),
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	} else {
		fmt.Printf("UnknownArtist - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	}
}

func audition(ctx context.Context) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Type: pbrg.RequestType_AUDITION})
	if err != nil {
		log.Fatalf("Error on get: %v", err)
	}
	if len(r.GetRecord().GetRelease().GetArtists()) > 0 {
		fmt.Printf("%v - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetArtists()[0].GetName(),
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	} else {
		fmt.Printf("UnknownArtist - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	}
}

func get(ctx context.Context, clean bool) {
	client, closer, err := dialRecordGetter(ctx)
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer closer()

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Refresh: true})
	if err != nil {
		log.Fatalf("Error on get: %v", err)
	}

	if clean {
		fmt.Fprintf(stdout, "%v\n", r.GetRecord().GetRelease().GetId())
		return
	}

	if len(r.GetRecord().GetRelease().GetArtists()) > 0 {
		fmt.Fprintf(stdout, "%v - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetArtists()[0].GetName(),
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	} else {
		fmt.Fprintf(stdout, "UnknownArtist - %v [%v] (%v/%v) {%v,%v}\n",
			r.GetRecord().GetRelease().GetTitle(),
			r.GetRecord().GetMetadata().GetCategory(),
			r.GetDisk(),
			r.GetRecord().GetRelease().GetFormatQuantity(),
			r.GetRecord().GetRelease().GetId(),
			r.GetRecord().GetRelease().GetInstanceId(),
		)
	}

	if r.GetRecord().GetMetadata() != nil && r.GetRecord().GetMetadata().GetCategory() == pbrc.ReleaseMetadata_STAGED_TO_SELL {
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		var digitalStatus string
		var scoreStatus string

		wg.Add(2)
		go func() {
			defer wg.Done()
			digitalStatus = fetchDigitalVersionsStatus(timeoutCtx, r.GetRecord().GetRelease())
		}()
		go func() {
			defer wg.Done()
			scoreStatus = fetchScoreHistoryStatus(timeoutCtx, r.GetRecord().GetRelease().GetInstanceId())
		}()
		wg.Wait()

		fmt.Fprintf(stdout, "  %s\n", formatRipStatus(r.GetRecord().GetMetadata()))
		fmt.Fprintf(stdout, "  %s\n", digitalStatus)
		fmt.Fprintf(stdout, "  %s\n", scoreStatus)
		fmt.Fprintf(stdout, "  %s\n", formatSalePrice(r.GetRecord().GetMetadata()))
	}
}

func score(ctx context.Context, value int32) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{})
	if err != nil {
		log.Fatalf("Error in scoring: %v", err)
	}
	if r.GetRecord().GetMetadata() == nil {
		r.GetRecord().Metadata = &pbrc.ReleaseMetadata{}
	}
	r.GetRecord().GetMetadata().SetRating = value
	_, err = client.Listened(ctx, r.GetRecord())
	if err != nil {
		fmt.Printf("%v", err)
	}
}

func scoreDigital(ctx context.Context, value int32) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Type: pbrg.RequestType_DIGITAL})
	if err != nil {
		log.Fatalf("Error in scoring: %v", err)
	}
	if r.GetRecord().GetMetadata() == nil {
		r.GetRecord().Metadata = &pbrc.ReleaseMetadata{}
	}
	r.GetRecord().GetMetadata().SetRating = value
	_, err = client.Listened(ctx, r.GetRecord())
	if err != nil {
		fmt.Printf("%v", err)
	}
}

func scoreCD(ctx context.Context, value int32) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Type: pbrg.RequestType_CD_FOCUS})
	if err != nil {
		log.Fatalf("Error in scoring: %v", err)
	}
	if r.GetRecord().GetMetadata() == nil {
		r.GetRecord().Metadata = &pbrc.ReleaseMetadata{}
	}
	r.GetRecord().GetMetadata().SetRating = value
	_, err = client.Listened(ctx, r.GetRecord())
	if err != nil {
		fmt.Printf("%v", err)
	}
}

func scoreAudition(ctx context.Context, score int32) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Type: pbrg.RequestType_AUDITION})
	if err != nil {
		log.Fatalf("Error in scoring: %v", err)
	}
	r.GetRecord().GetMetadata().AuditionScore = score
	_, err = client.Listened(ctx, r.GetRecord())
	if err != nil {
		fmt.Printf("%v", err)
	}
}

func main() {
	ctx, cancel := utils.ManualContext(fmt.Sprintf("recordgetter_cli-%v", os.Args[1]), time.Minute*5)
	defer cancel()

	if len(os.Args) == 1 {
		get(ctx, false)
	} else {
		switch os.Args[1] {
		case "clear":
			switch os.Args[2] {
			case "audition":
				clear(ctx, pbrg.RequestType_AUDITION)
			case "main":
				clear(ctx, pbrg.RequestType_DEFAULT)
			case "digital":
				clear(ctx, pbrg.RequestType_DIGITAL)
			}
		case "get":
			get(ctx, false)
				case "getc":
			get(ctx, true)
		case "score":
			val, err := strconv.ParseInt(os.Args[2], 10, 32)
			if err != nil {
				log.Fatalf("Error parsing num: %v", err)
			}
			score(ctx, int32(val))
		case "scoreaudition":
			val, err := strconv.ParseInt(os.Args[2], 10, 32)
			if err != nil {
				log.Fatalf("Error parsing num: %v", err)
			}
			scoreAudition(ctx, int32(val))
		case "audition":
			ctx, cancel = utils.ManualContext(fmt.Sprintf("recordgetter_cli-%v", os.Args[1]), time.Minute*30)
			defer cancel()

			audition(ctx)
		case "scoredigital":
			val, err := strconv.ParseInt(os.Args[2], 10, 32)
			if err != nil {
				log.Fatalf("Error parsing num: %v", err)
			}
			scoreDigital(ctx, int32(val))
		case "scorecd":
			val, err := strconv.ParseInt(os.Args[2], 10, 32)
			if err != nil {
				log.Fatalf("Error parsing num: %v", err)
			}
			scoreCD(ctx, int32(val))
		case "digital":
			ctx, cancel = utils.ManualContext(fmt.Sprintf("recordgetter_cli-%v", os.Args[1]), time.Minute*30)
			defer cancel()

			digital(ctx)
		case "cd":
			ctx, cancel = utils.ManualContext(fmt.Sprintf("recordgetter_cli-%v", os.Args[1]), time.Minute*30)
			defer cancel()

			cd(ctx)
		case "clean":
			val, err := strconv.ParseInt(os.Args[2], 10, 32)
			if err != nil {
				log.Fatalf("Error parsing num: %v", err)
			}
			clean(ctx, int64(val))
		default:
			fmt.Printf("Unknown command: %v\n", os.Args[1])
		}
	}
}
