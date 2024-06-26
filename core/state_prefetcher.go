// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

const prefetchThread = 4
const startupTxNum = prefetchThread
const checkInterval = 10

type prefetchTxRequest struct {
	slotIndex int
	txIndex   int
	tx        *types.Transaction
	to        common.Address // from      common.Address
	taken     int32          // false: has not been executed yet. true: executed
	eoaType   bool           // EOA account don't need to invoke EVM
}

type prefetchSlot struct {
	pendingTxReqList []*prefetchTxRequest
	wakeUpChan       chan struct{}
	noneEOATxs       int
}

// statePrefetcher is a basic Prefetcher, which blindly executes a block on top
// of an arbitrary state with the goal of prefetching potentially useful state
// data from disk before the main block processor start executing.
type statePrefetcher struct {
	config    *params.ChainConfig // Chain configuration options
	bc        *BlockChain         // Canonical block chain
	engine    consensus.Engine    // Consensus engine used for block rewards
	slots     []*prefetchSlot     // each slot represent a prefetch thread
	allTxReqs []*prefetchTxRequest
}

// NewStatePrefetcher initialises a new statePrefetcher.
func NewStatePrefetcher(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *statePrefetcher {
	return &statePrefetcher{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Benefits of StaticDispatch:
//
//	** try best to make Txs with same From() in same slot
//	** reduce IPC cost by dispatch in Unit
//	** make sure same From in same slot
//	** try to make it balanced, queue to the most hungry slot for new Address

// 1.dispatch the first $startupTxNum TXs first, they have the highest priority
// 2.filter out EOA transfer, get the address list on TXs iterate, and GetObject would be fine.
// 3.static dispatch txGroup with same ToAddress to same thread
// 4.run prefetch routine, stage 1
// 5.stage 2: idle thread? to preempt tasks from the busy thread??
//            easiest way: preempt the un-prefetch txGroup and run

// Prefetch processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to pre-cache transaction signatures and state trie nodes.
func (p *statePrefetcher) Prefetch(block *types.Block, statedb *state.StateDB, cfg *vm.Config, interruptCh <-chan struct{}) {
	var (
		header = block.Header()
		signer = types.MakeSigner(p.config, header.Number, header.Time)
	)
	transactions := block.Transactions()
	log.Info("Prefetch txGroup", "block", header.Number, "len(transactions)", len(transactions))
	// 1.start the prefetch threads
	p.slots = make([]*prefetchSlot, prefetchThread)
	for i := 0; i < prefetchThread; i++ {
		p.slots[i] = &prefetchSlot{
			wakeUpChan: make(chan struct{}, 1),
		}
		// start the primary slot's goroutine
		go func(slotIndex int) {
			curSlot := p.slots[slotIndex]
			newStatedb := statedb.CopyDoPrefetch()
			if !p.config.IsHertzfix(header.Number) {
				newStatedb.EnableWriteOnSharedStorage()
			}
			gaspool := new(GasPool).AddGas(block.GasLimit())
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, *cfg)
			// Iterate over and process the individual transactions
			for {
				select {
				case <-interruptCh:
					return
				case <-curSlot.wakeUpChan:
				}
				// stage 1:
				for _, txReq := range curSlot.pendingTxReqList {
					select {
					case <-interruptCh:
						// If block precaching was interrupted, abort
						return
					default:
					}
					atomic.StoreInt32(&txReq.taken, 1)
					log.Info("Prefetch tx", "thread", slotIndex, "txIndex", txReq.txIndex)
					// Convert the transaction into an executable message and pre-cache its sender
					msg, err := TransactionToMessage(txReq.tx, signer, header.BaseFee)
					msg.SkipAccountChecks = true
					if err != nil {
						log.Warn("TransactionToMessage", "error", err)
						return // Also invalid block, bail out
					}
					newStatedb.SetTxContext(txReq.tx.Hash(), txReq.txIndex)
					precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)
					log.Info("Prefetch tx done", "thread", slotIndex)
				}

				// stage 2: txReq in this Slot have all been executed, try preempt one from other slot.
				// as long as the TxReq is runnable, we steal it, mark it as stolen
				for _, txReq := range p.allTxReqs {
					select {
					case <-interruptCh:
						// If block precaching was interrupted, abort
						return
					default:
					}
					if atomic.CompareAndSwapInt32(&txReq.taken, 0, 1) {
						// Convert the transaction into an executable message and pre-cache its sender
						msg, err := TransactionToMessage(txReq.tx, signer, header.BaseFee)
						msg.SkipAccountChecks = true
						if err != nil {
							log.Warn("TransactionToMessage", "error", err)
							return // Also invalid block, bail out
						}
						newStatedb.SetTxContext(txReq.tx.Hash(), txReq.txIndex)
						precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)
						log.Info("Prefetch tx done", "thread", slotIndex)
					}
				}
			}
		}(i)
	}

	// 2.do static dispatch
	toSlotMap := make(map[common.Address]int, 100)
	for i, tx := range transactions {
		var toAddr common.Address
		if tx.To() == nil {
			// contract create,
			fromAddr, _ := types.Sender(signer, tx)
			toAddr = crypto.CreateAddress(fromAddr, tx.Nonce())
			log.Info("Prefetch contract create", "txHash", tx.Hash(), "fromAddr", fromAddr, "newAddress", toAddr)
		} else {
			toAddr = *tx.To()
		}
		txReq := &prefetchTxRequest{
			txIndex: i,
			tx:      tx,
			to:      toAddr,
			taken:   0,
			eoaType: false,
		}
		p.allTxReqs = append(p.allTxReqs, txReq)
		if len(tx.Data()) == 0 {
			txReq.eoaType = true
		}
	}
	// 2.1. the first $startupTxNum are special, to do, this can be optimized to avoid re-run in same slot
	for txIndex, txReq := range p.allTxReqs {
		if txIndex >= startupTxNum {
			break
		}
		slotIndex := txIndex % prefetchThread
		if _, ok := toSlotMap[txReq.to]; !ok {
			// already assigned to a slot.
			toSlotMap[txReq.to] = slotIndex
		}
		if !txReq.eoaType {
			p.slots[slotIndex].noneEOATxs++
		}
		slot := p.slots[slotIndex]
		txReq.slotIndex = slotIndex // txReq is better to be executed in this slot
		slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
	}
	// 2.2. dispatch all txs
	for _, txReq := range p.allTxReqs {
		if i, ok := toSlotMap[txReq.to]; ok {
			txReq.slotIndex = i
			if !txReq.eoaType {
				p.slots[i].noneEOATxs++
			}
		}

		var workload = p.slots[0].noneEOATxs
		slotIndex := 0
		for i, slot := range p.slots {
			if slot.noneEOATxs < workload {
				slotIndex = i
				workload = slot.noneEOATxs
			}
		}
		// update
		toSlotMap[txReq.to] = slotIndex
		slot := p.slots[slotIndex]
		txReq.slotIndex = slotIndex // txReq is better to be executed in this slot
		slot.pendingTxReqList = append(slot.pendingTxReqList, txReq)
	}
	// 3.after static dispatch, we notify the slot to work.
	for _, slot := range p.slots {
		slot.wakeUpChan <- struct{}{}
	}
}

// PrefetchMining processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to pre-cache transaction signatures and snapshot clean state. Only used for mining stage
func (p *statePrefetcher) PrefetchMining(txs TransactionsByPriceAndNonce, header *types.Header, gasLimit uint64, statedb *state.StateDB, cfg vm.Config, interruptCh <-chan struct{}, txCurr **types.Transaction) {
	var signer = types.MakeSigner(p.config, header.Number, header.Time)

	txCh := make(chan *types.Transaction, 2*prefetchThread)
	for i := 0; i < prefetchThread; i++ {
		go func(startCh <-chan *types.Transaction, stopCh <-chan struct{}) {
			idx := 0
			newStatedb := statedb.CopyDoPrefetch()
			if !p.config.IsHertzfix(header.Number) {
				newStatedb.EnableWriteOnSharedStorage()
			}
			gaspool := new(GasPool).AddGas(gasLimit)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
			// Iterate over and process the individual transactions
			for {
				select {
				case tx := <-startCh:
					// Convert the transaction into an executable message and pre-cache its sender
					msg, err := TransactionToMessage(tx, signer, header.BaseFee)
					msg.SkipAccountChecks = true
					if err != nil {
						return // Also invalid block, bail out
					}
					idx++
					newStatedb.SetTxContext(tx.Hash(), idx)
					precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)
					gaspool = new(GasPool).AddGas(gasLimit)
				case <-stopCh:
					return
				}
			}
		}(txCh, interruptCh)
	}
	go func(txset TransactionsByPriceAndNonce) {
		count := 0
		for {
			select {
			case <-interruptCh:
				return
			default:
				if count++; count%checkInterval == 0 {
					txset.Forward(*txCurr)
				}
				tx := txset.PeekWithUnwrap()
				if tx == nil {
					return
				}

				select {
				case <-interruptCh:
					return
				case txCh <- tx:
				}

				txset.Shift()
			}
		}
	}(txs)
}

// precacheTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. The goal is not to execute
// the transaction successfully, rather to warm up touched data slots.
func precacheTransaction(msg *Message, config *params.ChainConfig, gaspool *GasPool, statedb *state.StateDB, header *types.Header, evm *vm.EVM) error {
	// Update the evm with the new transaction context.
	evm.Reset(NewEVMTxContext(msg), statedb)
	// Add addresses to access list if applicable
	_, err := ApplyMessage(evm, msg, gaspool)
	if err == nil {
		statedb.Finalise(true)
	}
	return err
}
