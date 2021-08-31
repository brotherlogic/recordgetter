package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/brotherlogic/goserver/utils"
	"google.golang.org/grpc"

	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbrg "github.com/brotherlogic/recordgetter/proto"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
)

func findServer(name string) (string, int) {
	ip, port, _ := utils.Resolve(name, "recordgetter-cli")
	return ip, int(port)
}

func clear(ctx context.Context) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)
	r, err := client.Force(context.Background(), &pbrg.Empty{})
	fmt.Printf("%v and %v", r, err)
}

func listened(score int32) {
	host, port := findServer("recordgetter")
	conn, _ := grpc.Dial(host+":"+strconv.Itoa(port), grpc.WithInsecure())
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)
	r, err := client.GetRecord(context.Background(), &pbrg.GetRecordRequest{})
	if err != nil {
		log.Fatalf("%v", err)
	}
	r.GetRecord().GetRelease().Rating = score
	_, err = client.Listened(context.Background(), r.GetRecord())
	fmt.Printf("%v", err)
}

func audition(ctx context.Context) {
	conn, err := utils.LFDialServer(ctx, "recordgetter")
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Type: pbrg.GetRecordRequest_AUDITION})
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

func main() {
	ctx, cancel := utils.ManualContext(fmt.Sprintf("recordgetter_cli-%v", os.Args[1]), time.Minute*5)
	defer cancel()

	if len(os.Args) == 1 {
		get(ctx)
	} else {
		switch os.Args[1] {
		case "get":
			get(ctx)
		case "audition":
			audition(ctx)
		}
	}
}
