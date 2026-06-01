// Package heat は API アクセス数に基づく語彙の熱度（人気度）を Redis で管理する。
//
// 日毎のアクセス数を ZSET に記録し、バックグラウンドで
//
//	heat = w30 × (直近30日の総数) + w365 × (直近365日の総数)
//	       + wFreq × log(1 + Σmeta.frequencies)
//
// を集計して全語彙のランキング ZSET（genji:heat:index）を作る。
// 末尾の頻度項はコーパス出現回数の「下地」で、アクセスが少ない語の並び順を決める。
// Redis が無い場合は Noop（全て無効）にフォールバックする。
package heat

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	dayKeyPrefix     = "genji:hits:day:"
	entriesAllKey    = "genji:entries:all"
	freqKey          = "genji:heat:freq"
	indexKey         = "genji:heat:index"
	indexTmpKey      = "genji:heat:index:tmp"
	w30TmpKey        = "genji:heat:_w30"
	w365TmpKey       = "genji:heat:_w365"
	visitedTmpKey    = "genji:heat:_visited"
	seededVersionKey = "genji:heat:seeded_version"
	aggLockKey       = "genji:heat:agg:lock"

	dayKeyTTL  = 366 * 24 * time.Hour
	aggLockTTL = 10 * time.Minute // クラッシュ時のデッドロック防止用セーフティ
	seedBatch  = 5000
)

// Ranked は1語の熱度ランキング項目。
type Ranked struct {
	UUID string
	Heat float64
}

// SeedEntry は土台 seed 用の1語。FreqSum は meta.frequencies の合計出現回数。
type SeedEntry struct {
	UUID    string
	FreqSum int64
}

// Service は熱度カウンタ/ランキングの抽象。
type Service interface {
	// Enabled は Redis 連携が有効かを返す。
	Enabled() bool
	// Hit は指定 uuid 群のアクセスを記録する（非同期・ベストエフォート）。
	Hit(uuids ...string)
	// Page はランキング ZSET から offset/limit のページを返す（heat 降順）。総件数も返す。
	Page(ctx context.Context, offset, limit int) (items []Ranked, total int, err error)
	// Seed は DB バージョンが変わっていれば全 uuid を土台 ZSET に積む
	// （entries:all は 0 点、freq は log(1+頻度) 点）。
	Seed(ctx context.Context, entries []SeedEntry, version string) error
	// StartAggregator は集計ループを起動する（ctx 終了で停止）。
	StartAggregator(ctx context.Context)
}

// Noop は Redis 無効時の実装。
type Noop struct{}

func (Noop) Enabled() bool                                         { return false }
func (Noop) Hit(...string)                                         {}
func (Noop) Page(context.Context, int, int) ([]Ranked, int, error) { return nil, 0, nil }
func (Noop) Seed(context.Context, []SeedEntry, string) error       { return nil }
func (Noop) StartAggregator(context.Context)                       {}

// redisHeat は Redis を使う実装。
type redisHeat struct {
	client           *redis.Client
	w30, w365, wFreq float64
	aggInterval      time.Duration
}

// New は Service を構築する。client が nil なら Noop を返す。
func New(client *redis.Client, w30, w365, wFreq float64, aggInterval time.Duration) Service {
	if client == nil {
		return Noop{}
	}
	if aggInterval <= 0 {
		aggInterval = 15 * time.Minute
	}
	return &redisHeat{client: client, w30: w30, w365: w365, wFreq: wFreq, aggInterval: aggInterval}
}

func (h *redisHeat) Enabled() bool { return true }

func dayKey(t time.Time) string { return dayKeyPrefix + t.UTC().Format("20060102") }

// dayKeys は now を起点に直近 n 日分の day キーを返す。
func dayKeys(now time.Time, n int) []string {
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, dayKey(now.AddDate(0, 0, -i)))
	}
	return keys
}

func (h *redisHeat) Hit(uuids ...string) {
	if len(uuids) == 0 {
		return
	}
	// 呼び出し側のスライスを共有しないようコピーする。
	ids := make([]string, len(uuids))
	copy(ids, uuids)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		key := dayKey(time.Now())
		pipe := h.client.Pipeline()
		for _, u := range ids {
			pipe.ZIncrBy(ctx, key, 1, u)
		}
		pipe.Expire(ctx, key, dayKeyTTL)
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("heat: hit failed: %v", err)
		}
	}()
}

func (h *redisHeat) Page(ctx context.Context, offset, limit int) ([]Ranked, int, error) {
	total, err := h.client.ZCard(ctx, indexKey).Result()
	if err != nil {
		return nil, 0, err
	}
	if limit <= 0 || offset >= int(total) {
		return nil, int(total), nil
	}
	zs, err := h.client.ZRevRangeWithScores(ctx, indexKey, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, int(total), err
	}
	items := make([]Ranked, 0, len(zs))
	for _, z := range zs {
		uuid, _ := z.Member.(string)
		items = append(items, Ranked{UUID: uuid, Heat: z.Score})
	}
	return items, int(total), nil
}

func (h *redisHeat) Seed(ctx context.Context, entries []SeedEntry, version string) error {
	cur, err := h.client.Get(ctx, seededVersionKey).Result()
	if err == nil && cur == version && version != "" {
		return nil // 既に同バージョンで seed 済み
	}

	allTmp := entriesAllKey + ":tmp"
	freqTmp := freqKey + ":tmp"
	if err := h.client.Del(ctx, allTmp, freqTmp).Err(); err != nil {
		return err
	}
	for i := 0; i < len(entries); i += seedBatch {
		end := i + seedBatch
		if end > len(entries) {
			end = len(entries)
		}
		allMembers := make([]redis.Z, 0, end-i)
		freqMembers := make([]redis.Z, 0, end-i)
		for _, e := range entries[i:end] {
			allMembers = append(allMembers, redis.Z{Score: 0, Member: e.UUID})
			// log 正規化（log(1+n)）でアクセス数と桁を揃える。頻度なしは 0。
			freqMembers = append(freqMembers, redis.Z{Score: math.Log1p(float64(e.FreqSum)), Member: e.UUID})
		}
		if err := h.client.ZAdd(ctx, allTmp, allMembers...).Err(); err != nil {
			return err
		}
		if err := h.client.ZAdd(ctx, freqTmp, freqMembers...).Err(); err != nil {
			return err
		}
	}
	if len(entries) > 0 {
		if err := h.client.Rename(ctx, allTmp, entriesAllKey).Err(); err != nil {
			return err
		}
		if err := h.client.Rename(ctx, freqTmp, freqKey).Err(); err != nil {
			return err
		}
	}
	if err := h.client.Set(ctx, seededVersionKey, version, 0).Err(); err != nil {
		return err
	}
	log.Printf("heat: seeded %d entries (version=%s)", len(entries), version)
	return nil
}

func (h *redisHeat) StartAggregator(ctx context.Context) {
	go func() {
		// 起動直後に1回集計してから定期実行する。
		h.aggregate(ctx)
		t := time.NewTicker(h.aggInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				h.aggregate(ctx)
			}
		}
	}()
}

// aggregate は day バケットを集計して genji:heat:index を再構築する。
func (h *redisHeat) aggregate(ctx context.Context) {
	// 多重インスタンスでの重複集計を防ぐロック（終了時に解放）。
	ok, err := h.client.SetNX(ctx, aggLockKey, "1", aggLockTTL).Result()
	if err != nil {
		log.Printf("heat: agg lock error: %v", err)
		return
	}
	if !ok {
		return // 他インスタンスが集計中
	}
	defer h.client.Del(context.Background(), aggLockKey)

	now := time.Now().UTC()
	if err := h.client.ZUnionStore(ctx, w30TmpKey, &redis.ZStore{Keys: dayKeys(now, 30)}).Err(); err != nil {
		log.Printf("heat: agg w30 failed: %v", err)
		return
	}
	if err := h.client.ZUnionStore(ctx, w365TmpKey, &redis.ZStore{Keys: dayKeys(now, 365)}).Err(); err != nil {
		log.Printf("heat: agg w365 failed: %v", err)
		return
	}
	// heat = w30 × c30 + w365 × c365
	if err := h.client.ZUnionStore(ctx, visitedTmpKey, &redis.ZStore{
		Keys:    []string{w30TmpKey, w365TmpKey},
		Weights: []float64{h.w30, h.w365},
	}).Err(); err != nil {
		log.Printf("heat: agg visited failed: %v", err)
		return
	}
	// 全 uuid（土台 0 点）に頻度の下地と訪問分を重ねる。
	// heat = 0×(全uuid) + wFreq×log(1+頻度) + 1×(2×c30 + 1×c365)
	if err := h.client.ZUnionStore(ctx, indexTmpKey, &redis.ZStore{
		Keys:    []string{entriesAllKey, freqKey, visitedTmpKey},
		Weights: []float64{0, h.wFreq, 1},
	}).Err(); err != nil {
		log.Printf("heat: agg index failed: %v", err)
		return
	}

	// 原子的に差し替え。idxTmp が空（土台未 seed）なら index を空にする。
	if exists, _ := h.client.Exists(ctx, indexTmpKey).Result(); exists == 1 {
		if err := h.client.Rename(ctx, indexTmpKey, indexKey).Err(); err != nil {
			log.Printf("heat: agg rename failed: %v", err)
		}
	} else {
		h.client.Del(ctx, indexKey)
	}
	h.client.Del(ctx, w30TmpKey, w365TmpKey, visitedTmpKey)
}
