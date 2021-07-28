package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/brotherlogic/goserver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbgd "github.com/brotherlogic/godiscogs"
	pbg "github.com/brotherlogic/goserver/proto"
	"github.com/brotherlogic/goserver/utils"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbrg "github.com/brotherlogic/recordgetter/proto"
	pbro "github.com/brotherlogic/recordsorganiser/proto"
	rwpb "github.com/brotherlogic/recordwants/proto"
)

var (
	waiting = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "recordgetter_wait",
		Help: "Various Wait Times",
	}, []string{"wait"})
)

//Server main server type
type Server struct {
	*goserver.GoServer
	serving    bool
	delivering bool
	updater    updater
	rGetter    getter
	rd         *rand.Rand
	requests   int64
	lastPre    time.Time
	org        org
	wants      wants
}

const (
	wait = 5 * time.Second

	//KEY under which we store the collection
	KEY = "/github.com/brotherlogic/recordgetter/state"
)

type wants interface {
	getWants(ctx context.Context) ([]*rwpb.MasterWant, error)
	updateWant(ctx context.Context, id int32, level rwpb.MasterWant_Level) error
}

type prodWants struct {
	dial func(ctx context.Context, server string) (*grpc.ClientConn, error)
}

func (p *prodWants) getWants(ctx context.Context) ([]*rwpb.MasterWant, error) {
	conn, err := p.dial(ctx, "recordwants")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := rwpb.NewWantServiceClient(conn)
	wants, err := client.GetWants(ctx, &rwpb.GetWantsRequest{})
	if err != nil {
		return nil, err
	}
	return wants.GetWant(), nil
}

func (p *prodWants) updateWant(ctx context.Context, id int32, level rwpb.MasterWant_Level) error {
	conn, err := p.dial(ctx, "recordwants")
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rwpb.NewWantServiceClient(conn)
	_, err = client.Update(ctx, &rwpb.UpdateRequest{Want: &pbgd.Release{Id: id}, Level: level})
	return err
}

type org interface {
	getLocations(ctx context.Context) ([]*pbro.Location, error)
}

type prodOrg struct {
	dial func(ctx context.Context, server string) (*grpc.ClientConn, error)
}

func (p *prodOrg) getLocations(ctx context.Context) ([]*pbro.Location, error) {
	conn, err := p.dial(ctx, "recordsorganiser")
	if err != nil {
		return []*pbro.Location{}, err
	}
	defer conn.Close()
	client := pbro.NewOrganiserServiceClient(conn)
	locations, err := client.GetOrganisation(ctx, &pbro.GetOrganisationRequest{})
	if err != nil {
		return []*pbro.Location{}, err
	}

	return locations.Locations, err
}

type getter interface {
	getRelease(ctx context.Context, instanceID int32) (*pbrc.Record, error)
	getRecordsInCategory(ctx context.Context, category pbrc.ReleaseMetadata_Category) ([]int32, error)
	getRecordsInFolder(ctx context.Context, folder int32) ([]int32, error)
	getPlainRecord(ctx context.Context, id int32) (*pbrc.Record, error)
}

type prodGetter struct {
	dial func(ctx context.Context, server string) (*grpc.ClientConn, error)
	Log  func(s string)
}

func (p *prodGetter) getRecordsInCategory(ctx context.Context, category pbrc.ReleaseMetadata_Category) ([]int32, error) {
	conn, err := p.dial(ctx, "recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)

	r, err := client.QueryRecords(ctx, &pbrc.QueryRecordsRequest{Query: &pbrc.QueryRecordsRequest_Category{category}})
	if err == nil {
		return r.GetInstanceIds(), err
	}
	return []int32{}, err
}

func (p *prodGetter) getRecordsInFolder(ctx context.Context, folder int32) ([]int32, error) {
	conn, err := p.dial(ctx, "recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)

	r, err := client.QueryRecords(ctx, &pbrc.QueryRecordsRequest{Query: &pbrc.QueryRecordsRequest_FolderId{folder}})
	if err == nil {
		return r.GetInstanceIds(), err
	}
	return []int32{}, err
}

func (p *prodGetter) getRelease(ctx context.Context, instance int32) (*pbrc.Record, error) {
	conn, err := p.dial(ctx, "recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pbrc.NewRecordCollectionServiceClient(conn)
	r, err := client.GetRecord(ctx, &pbrc.GetRecordRequest{InstanceId: instance})
	if err != nil {
		return nil, err
	}
	return r.GetRecord(), err
}

func (p *prodGetter) getPlainRecord(ctx context.Context, id int32) (*pbrc.Record, error) {
	conn, err := p.dial(ctx, "recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pbrc.NewRecordCollectionServiceClient(conn)
	r, err := client.GetRecord(ctx, &pbrc.GetRecordRequest{ReleaseId: id})
	if err != nil {
		return nil, err
	}
	return r.GetRecord(), err
}

type updater interface {
	update(ctx context.Context, id, rating int32) error
}

type prodUpdater struct {
	dial func(ctx context.Context, server string) (*grpc.ClientConn, error)
}

func (p *prodUpdater) update(ctx context.Context, id, rating int32) error {
	conn, err := p.dial(ctx, "recordcollection")
	if err != nil {
		return err
	}

	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)
	_, err = client.UpdateRecord(ctx, &pbrc.UpdateRecordRequest{Update: &pbrc.Record{Release: &pbgd.Release{InstanceId: id}, Metadata: &pbrc.ReleaseMetadata{SetRating: rating}}, Reason: "RecordScore from Getter"})
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) dateFine(rc *pbrc.Record, t time.Time, state *pbrg.State) bool {
	// Don't listen to in box record
	if rc.GetMetadata().GetBoxState() != pbrc.ReleaseMetadata_BOX_UNKNOWN &&
		rc.GetMetadata().GetBoxState() != pbrc.ReleaseMetadata_OUT_OF_BOX {
		return false
	}

	s.Log(fmt.Sprintf("Found box state: (%v -> %v)", rc.GetRelease().GetInstanceId(), rc.GetMetadata().GetBoxState()))

	for _, score := range state.Scores {
		if score.InstanceId == rc.GetRelease().InstanceId {
			// Two days between listens
			if t.AddDate(0, 0, -2).Unix() <= score.ScoreDate {
				return false
			}
		}
	}
	return true
}

func (s *Server) getReleaseFromPile(ctx context.Context, state *pbrg.State, t time.Time) (*pbrc.Record, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	// Get a new record first
	rec, err := s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_UNLISTENED, state)
	if err != nil || rec != nil {
		return rec, err
	}

	// Look for pre high school records
	rec, err = s.getInFolderWithCategory(ctx, t, int32(673768), pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL, state)
	if err != nil || rec != nil {
		return rec, err
	}

	// Prioritise PRE_FRESHMAN if there's a lot of them.
	rec, err = s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_PRE_FRESHMAN, state)
	if err != nil || rec != nil {
		s.lastPre = time.Now()
		return rec, err
	}

	//Look for a record staged to sell
	rec, err = s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_STAGED_TO_SELL, state)
	if (err != nil || rec != nil) && s.validate(rec, state) {
		return rec, err
	}

	//Update the wait time
	waiting.With(prometheus.Labels{"wait": "want"}).Set(float64(state.GetLastWant()))

	// If it's been 6 hours since our last one, pull a want from the list
	if time.Now().Sub(time.Unix(state.GetLastWant(), 0)) > time.Hour*6 {
		wants, err := s.wants.getWants(ctx)
		if err != nil {
			return rec, err
		}
		for _, want := range wants {
			if want.Level == rwpb.MasterWant_UNKNOWN {
				rec, err := s.rGetter.getPlainRecord(ctx, want.GetRelease().GetId())
				if err == nil {
					return rec, err
				}

				code := status.Convert(err)
				if code.Code() != codes.Canceled {
					s.RaiseIssue("GetRecordError", fmt.Sprintf("Weird response back from record: %v", err))
				}
			}
		}
	}

	//Look for a record staged to sell
	rec, err = s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_PRE_VALIDATE, state)
	if (err != nil || rec != nil) && s.validate(rec, state) {
		return rec, err
	}

	//Look for a record staged to sell
	rec, err = s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_PRE_IN_COLLECTION, state)
	if (err != nil || rec != nil) && s.validate(rec, state) {
		return rec, err
	}

	pfTime := time.Hour * 3

	if t.Sub(s.lastPre) > pfTime {
		rec, err = s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_PRE_FRESHMAN, state)
		if err != nil || rec != nil {
			s.lastPre = time.Now()
			return rec, err
		}
	}

	// Look for pre distringuished 12" records
	for _, f := range []int32{242017} {
		rec, err = s.getInFolderWithCategory(ctx, t, f, pbrc.ReleaseMetadata_PRE_DISTINGUISHED, state)
		if err != nil || rec != nil {
			return rec, err
		}
	}

	rec, err = s.getInFolders(ctx, t, state.ActiveFolders, state)
	if err != nil || rec != nil {
		return rec, err
	}
	return nil, fmt.Errorf("Unable to locate record to listen to")
}

//Init a record getter
func Init() *Server {
	s := &Server{
		GoServer: &goserver.GoServer{},
		serving:  true, delivering: true,
		rd: rand.New(rand.NewSource(time.Now().Unix())),
	}
	s.updater = &prodUpdater{s.FDialServer}
	s.rGetter = &prodGetter{s.FDialServer, s.Log}
	s.org = &prodOrg{s.FDialServer}
	s.wants = &prodWants{s.FDialServer}
	s.Register = s
	s.PrepServer()
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {
	pbrg.RegisterRecordGetterServer(server, s)
}

// ReportHealth alerts if we're not healthy
func (s Server) ReportHealth() bool {
	return true
}

// Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*pbg.State {
	return []*pbg.State{
		&pbg.State{Key: "blah", Value: int64(12)},
	}
}

// This is the only method that interacts with disk
func (s *Server) loadState(ctx context.Context) (*pbrg.State, error) {
	state := &pbrg.State{}
	data, _, err := s.KSclient.Read(ctx, KEY, state)

	if err != nil {
		return nil, err
	}

	if data != nil {
		state = data.(*pbrg.State)
	}

	//Update the wait time
	waiting.With(prometheus.Labels{"wait": "want"}).Set(float64(state.GetLastWant()))

	return state, nil
}

func (s *Server) saveState(ctx context.Context, state *pbrg.State) error {
	if len(state.GetActiveFolders()) == 0 {
		return fmt.Errorf("Invalid state for saving: %v", state)
	}

	return s.KSclient.Save(ctx, KEY, state)
}

func main() {

	server := Init()

	err := server.RegisterServerV2("recordgetter", false, true)
	if err != nil {
		return
	}

	// Try to  update in play folders - best effort
	ctx, cancel := utils.ManualContext("rgload", time.Minute)
	state, err := server.loadState(ctx)
	if err == nil {
		err = server.readLocations(ctx, state)
		server.Log(fmt.Sprintf("Read locations: %v", err))
	} else {
		server.Log(fmt.Sprintf("Unable to load state: %v", err))
	}
	cancel()

	server.Serve()
}
