package informers

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

const (
	// streamPollInterval is how often a shard goroutine pauses when it has no
	// records to process.
	streamPollInterval = 2 * time.Second
	// streamRetryDelay is the backoff applied when a transient error occurs on
	// a shard iterator.
	streamRetryDelay = 5 * time.Second
	// shardDiscoveryInterval is how often the supervisor re-calls DescribeStream
	// to pick up newly created child shards during DynamoDB shard rotation.
	shardDiscoveryInterval = 30 * time.Second
	// getRecordsLimit is the maximum number of records to request per
	// GetRecords call. AWS maximum is 1000.
	getRecordsLimit = 1000
)

// shardState tracks per-shard polling state so that we can recover expired
// iterators without restarting from TRIM_HORIZON.
type shardState struct {
	shardID       string
	parentShardID string // empty for root shards; set for child shards
	iter          string // current shard iterator; empty means needs refresh
	lastSeqNum    string // last successfully processed sequence number
	errorCount    int    // consecutive errors; used for backoff decisions
}

// shardWorkerState holds the synchronization handle for a single shard
// goroutine so the supervisor can gate child shards on parent completion.
type shardWorkerState struct {
	done chan struct{} // closed when the goroutine exits (shard fully consumed or ctx canceled)
}

// dynamoDBStreamWatcher implements watch.Interface by tailing a DynamoDB
// Stream on a single table. It discovers the stream ARN from the table
// description, opens TRIM_HORIZON shard iterators, and delivers
// INSERT / MODIFY / REMOVE events to the result channel.
//
// Shard lifecycle:
//   - All current shards are discovered at startup; each is polled by its own
//     goroutine (Fix 4: parallelism).
//   - A child shard goroutine waits until its parent goroutine has finished
//     before starting to poll (Fix 1: parent-before-child ordering).
//   - When a parent shard closes, the goroutine signals the supervisor via
//     rediscoverCh. The supervisor immediately calls DescribeStream with
//     ShardFilter{CHILD_SHARDS} to find and launch children without waiting
//     for the 30s periodic tick (Fix 2: targeted child discovery).
//
// GetRecords is called with an explicit Limit of 1000 (Fix 3).
//
// Expired iterator recovery: when an iterator expires the watcher re-obtains
// one using AFTER_SEQUENCE_NUMBER.
type dynamoDBStreamWatcher struct {
	resultCh chan watch.Event
	done     chan struct{}
	cancel   context.CancelFunc
}

func newDynamoDBStreamWatcher(
	ctx context.Context,
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	tableName string,
	convertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
) *dynamoDBStreamWatcher {
	ctx, cancel := context.WithCancel(ctx)
	w := &dynamoDBStreamWatcher{
		resultCh: make(chan watch.Event, 100),
		done:     make(chan struct{}),
		cancel:   cancel,
	}
	klog.V(4).InfoS("stream watcher starting", "table", tableName)
	go w.run(ctx, dbClient, streamsClient, tableName, convertFn)
	return w
}

func (w *dynamoDBStreamWatcher) run(
	ctx context.Context,
	dbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	tableName string,
	convertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
) {
	defer close(w.done)
	defer close(w.resultCh)

	streamARN, err := getTableStreamARN(ctx, dbClient, tableName)
	if err != nil {
		if ctx.Err() == nil {
			klog.ErrorS(err, "stream watcher failed to get stream ARN", "table", tableName)
			w.sendError(ctx, err)
		}
		return
	}
	if streamARN == "" {
		klog.V(2).InfoS("stream watcher: streams not enabled on table, watch is a no-op", "table", tableName)
		return
	}
	klog.V(4).InfoS("stream watcher connected", "table", tableName, "streamARN", streamARN)

	// workers maps shardID → its goroutine handle.
	// Written only by the supervisor goroutine; done channels read by shard goroutines.
	workers := make(map[string]*shardWorkerState)
	var mu sync.RWMutex

	// rediscoverCh is sent to by shard goroutines when they close, carrying
	// the closed shard's ID so the supervisor can immediately fetch its
	// children via ShardFilter (Fix 2).
	rediscoverCh := make(chan string, 32)

	launchShard := func(s shardState) {
		ws := &shardWorkerState{done: make(chan struct{})}
		mu.Lock()
		workers[s.shardID] = ws
		mu.Unlock()
		go w.pollShard(ctx, streamsClient, streamARN, tableName, s, ws, &mu, workers, convertFn, rediscoverCh)
	}

	// Initial full topology discovery.
	shards, err := discoverAllShards(ctx, streamsClient, streamARN)
	if err != nil {
		if ctx.Err() == nil {
			klog.ErrorS(err, "stream watcher failed to discover shards", "table", tableName)
			w.sendError(ctx, err)
		}
		return
	}
	klog.V(4).InfoS("stream watcher discovered shards", "table", tableName, "shards", len(shards))
	for _, s := range shards {
		launchShard(s)
	}

	// Supervisor loop: launch goroutines for newly discovered child shards.
	// Triggered either by a shard-close signal (immediate, via ShardFilter)
	// or by the periodic ticker (catches any shards missed by the signal path).
	ticker := time.NewTicker(shardDiscoveryInterval)
	defer ticker.Stop()

	discoverAndLaunchNew := func() {
		mu.RLock()
		knownIDs := make(map[string]struct{}, len(workers))
		for id := range workers {
			knownIDs[id] = struct{}{}
		}
		mu.RUnlock()

		newShards, err := discoverAllShards(ctx, streamsClient, streamARN)
		if err != nil {
			if ctx.Err() == nil {
				klog.V(2).InfoS("stream watcher failed to re-discover shards, will retry", "table", tableName, "err", err)
			}
			return
		}
		for _, s := range newShards {
			if _, known := knownIDs[s.shardID]; !known {
				klog.V(2).InfoS("stream watcher launching new shard", "table", tableName, "shardID", s.shardID, "parentShardID", s.parentShardID)
				launchShard(s)
			}
		}
	}

	// discoverChildrenOf uses ShardFilter to fetch only the children of a
	// specific closed parent shard (Fix 2), launching any not yet known.
	discoverChildrenOf := func(parentShardID string) {
		mu.RLock()
		knownIDs := make(map[string]struct{}, len(workers))
		for id := range workers {
			knownIDs[id] = struct{}{}
		}
		mu.RUnlock()

		children, err := discoverChildShards(ctx, streamsClient, streamARN, parentShardID)
		if err != nil {
			if ctx.Err() == nil {
				klog.V(2).InfoS("stream watcher failed to discover child shards via ShardFilter, falling back to full scan", "table", tableName, "parentShardID", parentShardID, "err", err)
				discoverAndLaunchNew()
			}
			return
		}
		for _, child := range children {
			if _, known := knownIDs[child.shardID]; !known {
				klog.V(2).InfoS("stream watcher launching child shard", "table", tableName, "shardID", child.shardID, "parentShardID", parentShardID)
				launchShard(child)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case parentID := <-rediscoverCh:
			// A shard closed — immediately discover its children (Fix 2).
			discoverChildrenOf(parentID)
		case <-ticker.C:
			// Periodic safety net: catches any shards missed by the signal path
			// (e.g. if ShardFilter returned an error or a race with initial load).
			discoverAndLaunchNew()
		}
	}
}

// pollShard is the per-shard goroutine. It:
//  1. Waits for its parent shard goroutine to finish (Fix 1).
//  2. Polls GetRecords in a loop, delivering events to w.resultCh (Fix 4).
//  3. On close, signals the supervisor via rediscoverCh (Fix 2).
func (w *dynamoDBStreamWatcher) pollShard(
	ctx context.Context,
	streamsClient *dynamodbstreams.Client,
	streamARN string,
	tableName string,
	s shardState,
	ws *shardWorkerState,
	mu *sync.RWMutex,
	workers map[string]*shardWorkerState,
	convertFn func(map[string]streamtypes.AttributeValue) (runtime.Object, error),
	rediscoverCh chan<- string,
) {
	defer close(ws.done)

	// Fix 1: Wait for parent to finish before we start polling.
	if s.parentShardID != "" {
		mu.RLock()
		parentWS, parentExists := workers[s.parentShardID]
		mu.RUnlock()

		if parentExists {
			klog.V(4).InfoS("stream watcher shard waiting for parent", "table", tableName, "shardID", s.shardID, "parentShardID", s.parentShardID)
			select {
			case <-parentWS.done:
			case <-ctx.Done():
				return
			}
			klog.V(4).InfoS("stream watcher shard parent done, starting poll", "table", tableName, "shardID", s.shardID)
		}
	}

	// notifyClosed sends the closed shard's ID to the supervisor so it can
	// immediately discover children using ShardFilter (Fix 2). Non-blocking:
	// if the channel is full the supervisor's periodic ticker is the fallback.
	notifyClosed := func() {
		select {
		case rediscoverCh <- s.shardID:
		default:
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// Refresh iterator if needed.
		if s.iter == "" {
			refreshed, err := refreshShardIterator(ctx, streamsClient, streamARN, &s)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				klog.V(2).InfoS("stream watcher failed to refresh shard iterator, will retry", "table", tableName, "shardID", s.shardID, "err", err)
				s.errorCount++
				select {
				case <-ctx.Done():
					return
				case <-time.After(streamRetryDelay):
				}
				continue
			}
			if !refreshed {
				// Shard is closed and fully consumed.
				klog.V(4).InfoS("stream watcher shard closed", "table", tableName, "shardID", s.shardID)
				notifyClosed()
				return
			}
		}

		records, nextIter, err := getRecords(ctx, streamsClient, s.iter)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			klog.V(2).InfoS("stream watcher error reading shard, will retry after delay", "table", tableName, "shardID", s.shardID, "err", err)
			s.errorCount++
			s.iter = "" // force iterator refresh on next pass
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamRetryDelay):
			}
			continue
		}
		s.errorCount = 0

		for _, rec := range records {
			if rec.Dynamodb == nil {
				continue
			}
			if rec.Dynamodb.SequenceNumber != nil {
				s.lastSeqNum = *rec.Dynamodb.SequenceNumber
			}

			var image map[string]streamtypes.AttributeValue
			var eventType watch.EventType
			switch rec.EventName {
			case streamtypes.OperationTypeInsert:
				eventType = watch.Added
				image = rec.Dynamodb.NewImage
			case streamtypes.OperationTypeModify:
				eventType = watch.Modified
				image = rec.Dynamodb.NewImage
			case streamtypes.OperationTypeRemove:
				eventType = watch.Deleted
				image = rec.Dynamodb.OldImage
			default:
				continue
			}
			if image == nil {
				continue
			}
			obj, err := convertFn(image)
			if err != nil {
				klog.V(4).InfoS("stream watcher skipping unconvertible record", "table", tableName, "eventName", rec.EventName, "err", err)
				continue
			}
			if accessor, ok := obj.(metav1.ObjectMetaAccessor); ok {
				klog.V(2).InfoS("stream watcher delivering event", "table", tableName, "eventType", eventType, "documentID", accessor.GetObjectMeta().GetName())
			}
			select {
			case w.resultCh <- watch.Event{Type: eventType, Object: obj}:
			case <-ctx.Done():
				return
			}
		}

		// nextIter empty means the shard is closed (all records consumed).
		if nextIter == "" {
			klog.V(4).InfoS("stream watcher shard exhausted", "table", tableName, "shardID", s.shardID)
			notifyClosed()
			return
		}
		s.iter = nextIter

		if len(records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(streamPollInterval):
			}
		} else {
			// Brief yield to avoid hot-looping under sustained load.
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// discoverAllShards pages through the full DescribeStream topology and returns
// a shardState for every shard, preserving parentShardID relationships.
// No shard iterators are opened here; that is deferred to pollShard.
func discoverAllShards(ctx context.Context, streamsClient *dynamodbstreams.Client, streamARN string) ([]shardState, error) {
	var result []shardState
	var lastShardID *string
	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamARN),
			ExclusiveStartShardId: lastShardID,
		}
		out, err := streamsClient.DescribeStream(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, shard := range out.StreamDescription.Shards {
			if shard.ShardId == nil {
				continue
			}
			s := shardState{shardID: *shard.ShardId}
			if shard.ParentShardId != nil {
				s.parentShardID = *shard.ParentShardId
			}
			result = append(result, s)
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return result, nil
}

// discoverChildShards uses the ShardFilter API (available since SDK v1.26.0)
// to fetch only the children of the given closed parent shard (Fix 2). This
// avoids a full DescribeStream topology scan on every shard close event.
func discoverChildShards(ctx context.Context, streamsClient *dynamodbstreams.Client, streamARN string, parentShardID string) ([]shardState, error) {
	var result []shardState
	var lastShardID *string
	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamARN),
			ExclusiveStartShardId: lastShardID,
			ShardFilter: &streamtypes.ShardFilter{
				Type:    streamtypes.ShardFilterTypeChildShards,
				ShardId: aws.String(parentShardID),
			},
		}
		out, err := streamsClient.DescribeStream(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, shard := range out.StreamDescription.Shards {
			if shard.ShardId == nil {
				continue
			}
			result = append(result, shardState{
				shardID:       *shard.ShardId,
				parentShardID: parentShardID,
			})
		}
		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		lastShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return result, nil
}

// refreshShardIterator obtains a new iterator for a shard whose current
// iterator has been cleared (expired or end-of-shard). Uses
// AFTER_SEQUENCE_NUMBER when available, otherwise TRIM_HORIZON.
// Returns (true, nil) if a new iterator was obtained, (false, nil) if the
// shard is fully closed, or (false, err) on error.
func refreshShardIterator(ctx context.Context, streamsClient *dynamodbstreams.Client, streamARN string, s *shardState) (bool, error) {
	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn: aws.String(streamARN),
		ShardId:   aws.String(s.shardID),
	}
	if s.lastSeqNum != "" {
		input.ShardIteratorType = streamtypes.ShardIteratorTypeAfterSequenceNumber
		input.SequenceNumber = aws.String(s.lastSeqNum)
	} else {
		input.ShardIteratorType = streamtypes.ShardIteratorTypeTrimHorizon
	}
	out, err := streamsClient.GetShardIterator(ctx, input)
	if err != nil {
		return false, err
	}
	if out.ShardIterator == nil {
		// Shard is closed and fully consumed.
		return false, nil
	}
	s.iter = *out.ShardIterator
	return true, nil
}

// getTableStreamARN retrieves the latest stream ARN for a table (empty string
// if Streams are not enabled).
func getTableStreamARN(ctx context.Context, dbClient *dynamodb.Client, tableName string) (string, error) {
	out, err := dbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return "", err
	}
	if out.Table.LatestStreamArn == nil {
		return "", nil
	}
	return *out.Table.LatestStreamArn, nil
}

// getRecords calls GetRecords for one shard iterator with an explicit Limit
// (Fix 3) and returns the records plus the next iterator (empty string when
// the shard is exhausted).
func getRecords(ctx context.Context, streamsClient *dynamodbstreams.Client, shardIterator string) ([]streamtypes.Record, string, error) {
	out, err := streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(shardIterator),
		Limit:         aws.Int32(getRecordsLimit),
	})
	if err != nil {
		return nil, "", err
	}
	nextIter := ""
	if out.NextShardIterator != nil {
		nextIter = *out.NextShardIterator
	}
	return out.Records, nextIter, nil
}

func (w *dynamoDBStreamWatcher) sendError(ctx context.Context, err error) {
	event := watch.Event{
		Type: watch.Error,
		Object: &metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusGone,
			Reason:  metav1.StatusReasonExpired,
			Message: err.Error(),
		},
	}
	select {
	case w.resultCh <- event:
	case <-ctx.Done():
	}
}

func (w *dynamoDBStreamWatcher) Stop() {
	w.cancel()
}

func (w *dynamoDBStreamWatcher) ResultChan() <-chan watch.Event {
	return w.resultCh
}
