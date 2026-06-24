package consensus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func quotaUps(specs ...struct {
	id   string
	tags []string
}) []common.Upstream {
	out := make([]common.Upstream, len(specs))
	for i, s := range specs {
		out[i] = common.NewFakeUpstream(s.id, common.WithTags(s.tags...))
	}
	return out
}

func idsOf(ups []common.Upstream) []string {
	out := make([]string, len(ups))
	for i, u := range ups {
		out[i] = u.Id()
	}
	return out
}

func TestReorderForParticipantQuota(t *testing.T) {
	type spec = struct {
		id   string
		tags []string
	}

	t.Run("disabled: nil requirements is a no-op", func(t *testing.T) {
		ups := quotaUps(spec{"a", nil}, spec{"b", nil})
		got := reorderForParticipantQuota(ups, nil)
		require.Equal(t, []string{"a", "b"}, idsOf(got))
	})

	t.Run("promotes a tag match into the front window", func(t *testing.T) {
		// Selection order puts the us-east upstream last; a min-1 quota on
		// region:us must pull it forward so a small maxParticipants includes it.
		ups := quotaUps(
			spec{"fast-eu", []string{"region:eu"}},
			spec{"mid-eu", []string{"region:eu"}},
			spec{"slow-us", []string{"region:us"}},
		)
		reqs := []*common.ConsensusRequiredParticipant{{Tag: "region:us", MinParticipants: 1}}
		got := idsOf(reorderForParticipantQuota(ups, reqs))
		require.Equal(t, "slow-us", got[0], "the only region:us upstream must be front-loaded")
		require.ElementsMatch(t, []string{"fast-eu", "mid-eu", "slow-us"}, got, "no upstream lost or duplicated")
	})

	t.Run("preserves order when quota already satisfied at the front", func(t *testing.T) {
		ups := quotaUps(
			spec{"us1", []string{"region:us"}},
			spec{"eu1", []string{"region:eu"}},
			spec{"us2", []string{"region:us"}},
		)
		reqs := []*common.ConsensusRequiredParticipant{{Tag: "region:us", MinParticipants: 1}}
		// us1 already matches at position 0 → no reordering needed.
		require.Equal(t, []string{"us1", "eu1", "us2"}, idsOf(reorderForParticipantQuota(ups, reqs)))
	})

	t.Run("min>1 promotes multiple matches in incoming order", func(t *testing.T) {
		ups := quotaUps(
			spec{"eu1", []string{"region:eu"}},
			spec{"us1", []string{"region:us"}},
			spec{"eu2", []string{"region:eu"}},
			spec{"us2", []string{"region:us"}},
		)
		reqs := []*common.ConsensusRequiredParticipant{{Tag: "region:us", MinParticipants: 2}}
		got := idsOf(reorderForParticipantQuota(ups, reqs))
		require.Equal(t, []string{"us1", "us2", "eu1", "eu2"}, got)
	})

	t.Run("best-effort when group smaller than min", func(t *testing.T) {
		ups := quotaUps(
			spec{"eu1", []string{"region:eu"}},
			spec{"us1", []string{"region:us"}},
		)
		reqs := []*common.ConsensusRequiredParticipant{{Tag: "region:us", MinParticipants: 3}}
		got := idsOf(reorderForParticipantQuota(ups, reqs))
		require.Equal(t, []string{"us1", "eu1"}, got, "promotes the one match it has; rest follow")
	})

	t.Run("multiple requirements front-load both groups", func(t *testing.T) {
		ups := quotaUps(
			spec{"public1", []string{"tier:public"}},
			spec{"public2", []string{"tier:public"}},
			spec{"paid1", []string{"tier:paid"}},
			spec{"region-us", []string{"region:us", "tier:public"}},
		)
		reqs := []*common.ConsensusRequiredParticipant{
			{Tag: "tier:paid", MinParticipants: 1},
			{Tag: "region:us", MinParticipants: 1},
		}
		got := idsOf(reorderForParticipantQuota(ups, reqs))
		// paid1 promoted for the first req, region-us for the second; the two
		// tier:public upstreams keep their order behind them.
		require.Equal(t, []string{"paid1", "region-us", "public1", "public2"}, got)
	})

	t.Run("glob tag pattern matches", func(t *testing.T) {
		ups := quotaUps(
			spec{"eu", []string{"region:eu-west"}},
			spec{"us", []string{"region:us-east"}},
		)
		reqs := []*common.ConsensusRequiredParticipant{{Tag: "region:us-*", MinParticipants: 1}}
		require.Equal(t, []string{"us", "eu"}, idsOf(reorderForParticipantQuota(ups, reqs)))
	})

	t.Run("no match leaves the list untouched", func(t *testing.T) {
		ups := quotaUps(spec{"a", []string{"region:eu"}}, spec{"b", []string{"region:eu"}})
		reqs := []*common.ConsensusRequiredParticipant{{Tag: "region:ap", MinParticipants: 1}}
		require.Equal(t, []string{"a", "b"}, idsOf(reorderForParticipantQuota(ups, reqs)))
	})
}

func TestRequiredParticipantQuotaFailsClosedAtAnalysis(t *testing.T) {
	lg := zerolog.Nop()
	cfg := &config{
		maxParticipants:         1,
		agreementThreshold:      1,
		disputeBehavior:         common.ConsensusDisputeBehaviorReturnError,
		lowParticipantsBehavior: common.ConsensusLowParticipantsBehaviorAcceptMostCommonValidResult,
		requiredParticipants: []*common.ConsensusRequiredParticipant{
			{Tag: "tier:paid", MinParticipants: 1},
		},
	}

	t.Run("missing required tag rejects threshold winner", func(t *testing.T) {
		analysis := newConsensusAnalysis(&lg, context.Background(), cfg, []*execResult{{
			Result:   validResponseWithValue("0x1"),
			Upstream: common.NewFakeUpstream("public-1", common.WithTags("tier:public")),
		}})

		winner := (&executor{}).determineWinner(&lg, analysis)
		require.NotNil(t, winner)
		require.Nil(t, winner.Result)
		require.Error(t, winner.Error)
		require.True(t, common.HasErrorCode(winner.Error, common.ErrCodeConsensusLowParticipants), "got: %v", winner.Error)
	})

	t.Run("required tag present allows threshold winner", func(t *testing.T) {
		analysis := newConsensusAnalysis(&lg, context.Background(), cfg, []*execResult{{
			Result:   validResponseWithValue("0x1"),
			Upstream: common.NewFakeUpstream("paid-1", common.WithTags("tier:paid")),
		}})

		winner := (&executor{}).determineWinner(&lg, analysis)
		require.NotNil(t, winner)
		require.NoError(t, winner.Error)
		require.NotNil(t, winner.Result)
	})
}

func TestRequiredParticipantQuotaWaitsForSlowRequiredParticipant(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	paid := common.NewFakeUpstream("paid-1", common.WithTags("tier:paid"))
	public1 := common.NewFakeUpstream("public-1", common.WithTags("tier:public"))
	public2 := common.NewFakeUpstream("public-2", common.WithTags("tier:public"))

	pol := newBuilder().
		WithLogger(&logger).
		WithMaxParticipants(3).
		WithAgreementThreshold(2).
		WithDisputeBehavior(common.ConsensusDisputeBehaviorAcceptMostCommonValidResult).
		WithLowParticipantsBehavior(common.ConsensusLowParticipantsBehaviorAcceptMostCommonValidResult).
		WithMaxWaitOnResult(common.NewStaticDuration(500 * time.Millisecond)).
		WithMaxWaitOnEmpty(common.NewStaticDuration(500 * time.Millisecond)).
		WithRequiredParticipants([]*common.ConsensusRequiredParticipant{
			{Tag: "tier:paid", MinParticipants: 1},
		}).
		Build()

	req := newTestRequest()
	req.SetUpstreams([]common.Upstream{public1, public2, paid})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var calls atomic.Int32
	var paidCompleted atomic.Bool
	resp, err := pol.Run(ctx, req, func(ctx context.Context, _ *common.NormalizedRequest) (*common.NormalizedResponse, error) {
		switch calls.Add(1) {
		case 1:
			select {
			case <-time.After(120 * time.Millisecond):
				paidCompleted.Store(true)
				return validResponseWithValue("0x1").SetUpstream(paid), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case 2:
			return validResponseWithValue("0x1").SetUpstream(public1), nil
		default:
			return validResponseWithValue("0x1").SetUpstream(public2), nil
		}
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, paidCompleted.Load(), "slow required participant must be allowed to finish")
	require.GreaterOrEqual(t, calls.Load(), int32(3), "third participant should start before quota is satisfied")
}
