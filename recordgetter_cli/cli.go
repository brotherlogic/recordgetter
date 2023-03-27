package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/brotherlogic/goserver/utils"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbrg "github.com/brotherlogic/recordgetter/proto"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
)

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

func get(ctx context.Context) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Refresh: true})
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
		get(ctx)
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
			get(ctx)
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
		case "digital":
			ctx, cancel = utils.ManualContext(fmt.Sprintf("recordgetter_cli-%v", os.Args[1]), time.Minute*30)
			defer cancel()

			digital(ctx)
		default:
			fmt.Printf("Unknown command: %v\n", os.Args[1])
		}
	}
}
