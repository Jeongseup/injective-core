package server

import (
	"context"
	"time"

	chaintypes "github.com/InjectiveLabs/injective-core/injective-chain/types"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/service"
	rpcclient "github.com/cometbft/cometbft/rpc/client"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/cometbft/cometbft/types"
)

const (
	ServiceName = "EVMIndexerService"

	NewBlockWaitTimeout = 60 * time.Second

	// https://github.com/cometbft/cometbft/blob/v0.37.4/rpc/core/env.go#L193
	NotFoundErr          = "is not available"
	ErrorBackoffDuration = 1 * time.Second
)

// EVMIndexerService indexes transactions for json-rpc service.
type EVMIndexerService struct {
	service.BaseService

	txIdxr   chaintypes.EVMTxIndexer
	client   rpcclient.Client
	allowGap bool
}

// NewEVMIndexerService returns a new service instance.
func NewEVMIndexerService(
	txIdxr chaintypes.EVMTxIndexer,
	client rpcclient.Client,
	allowGap bool,
	logger log.Logger,
) *EVMIndexerService {
	is := &EVMIndexerService{txIdxr: txIdxr, client: client, allowGap: allowGap}
	is.BaseService = *service.NewBaseService(logger, ServiceName, is)
	return is
}

// OnStart implements service.Service by subscribing for new blocks
// and indexing them by events.
func (eis *EVMIndexerService) OnStart() error {
	ctx := context.Background()
	status, err := eis.client.Status(ctx)
	if err != nil {
		return err
	}
	latestBlock := status.SyncInfo.LatestBlockHeight
	newBlockSignal := make(chan struct{}, 1)

	// Use SubscribeUnbuffered here to ensure both subscriptions does not get
	// canceled due to not pulling messages fast enough. Cause this might
	// sometimes happen when there are no other subscribers.
	blockHeadersChan, err := eis.client.Subscribe(
		ctx,
		ServiceName,
		types.QueryForEvent(types.EventNewBlockHeader).String(),
		0)
	if err != nil {
		return err
	}

	go func() {
		for {
			msg := <-blockHeadersChan
			eventDataHeader := msg.Data.(types.EventDataNewBlockHeader)
			if eventDataHeader.Header.Height > latestBlock {
				eis.Logger.Error("✅ got new event data header", "height", eventDataHeader.Header.Height)
				latestBlock = eventDataHeader.Header.Height
				// notify
				select {
				case newBlockSignal <- struct{}{}:
				default:
				}
			}
		}
	}()

	lastBlock, err := eis.txIdxr.LastIndexedBlock()
	if err != nil {
		return err
	}
	if lastBlock == -1 {
		lastBlock = latestBlock
		eis.Logger.Error("❗️ lastBlock == -1 입니다.", "height", latestBlock)
	} else if lastBlock < status.SyncInfo.EarliestBlockHeight {
		if !eis.allowGap {
			panic("Block gap detected, please recover the missing data")
		}
		// to avoid infinite failed to fetch block error when lastBlock is smaller than earliest
		lastBlock = status.SyncInfo.EarliestBlockHeight
		eis.Logger.Error("❗️ lastBlock < status.SyncInfo.EarliestBlockHeight 입니다.", "height", latestBlock)
	}
	// to avoid height must be greater than 0 error
	if lastBlock <= 0 {
		lastBlock = 1
		eis.Logger.Error("❗️ lastBlock 이 0보다 작어서 1이 됩니다.", "height", latestBlock)

	}

	for {
		if latestBlock <= lastBlock {
			// nothing to index. wait for signal of new block

			select {
			case <-newBlockSignal:
			case <-time.After(NewBlockWaitTimeout):
			}
			eis.Logger.Error("❗️ latestBlock <= lastBlock 때문에 로직이 스킵", "height", latestBlock)

			continue
		}
		var (
			err         error
			block       *ctypes.ResultBlock
			blockResult *ctypes.ResultBlockResults
		)
		for i := lastBlock + 1; i <= latestBlock; i++ {
			// 1. Block 가져오기 및 유효성 검사
			block, err = eis.client.Block(ctx, &i)
			if err != nil || block == nil {
				// 오류가 발생했거나, 오류는 없지만 block 객체가 nil인 경우
				eis.Logger.Error("⚠️ failed to fetch block or block is nil, skipping height", "height", i, "err", err)
				continue // 다음 높이로 넘어갑니다.
			}
			eis.Logger.Info("✅ received new block", "height", i)

			// 2. BlockResults 가져오기 및 유효성 검사
			blockResult, err = eis.client.BlockResults(ctx, &i)
			if err != nil || blockResult == nil {
				// 오류가 발생했거나, 오류는 없지만 blockResult 객체가 nil인 경우
				eis.Logger.Error("⚠️ failed to fetch block result or result is nil, skipping height", "height", i, "err", err)
				continue // 다음 높이로 넘어갑니다.
			}
			eis.Logger.Info("✅ received new block results", "height", i)

			// 3. 모든 데이터가 유효한 경우에만 인덱싱 수행
			if err := eis.txIdxr.IndexBlock(block.Block, blockResult.TxResults); err != nil {
				eis.Logger.Error("failed to index block", "height", i, "err", err)
				// 인덱싱 실패 시에도 일단 다음 블록으로 넘어갑니다.
				// 만약 인덱싱 실패 시 멈춰야 한다면 여기에 'break'를 추가하세요.
				eis.Logger.Info("⚠️ failed to updated IndexBlock but skipped the block", "height", lastBlock)
			} else {
				// 인덱싱이 성공한 경우에만 lastBlock을 업데이트합니다.
				lastBlock = blockResult.Height
				eis.Logger.Info("✅ updated lastBlock", "height", lastBlock)
			}
		}
		if err != nil {
			time.Sleep(ErrorBackoffDuration)
		}
	}
}
