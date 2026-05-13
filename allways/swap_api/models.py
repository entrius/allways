"""Pydantic request/response shapes for swap-api endpoints (spec §6)."""

from typing import List, Optional, Tuple

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    ok: bool
    chainBlock: Optional[int] = Field(None, description='Current subtensor block, when reachable')
    contractAddress: str


class ChainInfo(BaseModel):
    id: str
    name: str
    decimals: int
    native_unit: str


class ChainsResponse(BaseModel):
    chains: List[ChainInfo]
    pairs: List[Tuple[str, str]] = Field(..., description='Every supported (from, to) ordering')


class MinerSummary(BaseModel):
    hotkey: str
    rate: str = Field(..., description='Canonical rate string for the requested direction')
    collateralRao: int
    isActive: bool
    hasActiveSwap: bool


class BestMinerResponse(BaseModel):
    minerHotkey: str
    rate: str
    expectedOut: int = Field(..., description='Gross dest amount before fee, in smallest unit')
    reservationCapacity: int = Field(..., description="Miner's collateral in rao — caps swap size")
    sourceAddress: str = Field(..., description='Address users send source funds to')
    freshAsOf: int = Field(..., description='Subtensor block when this quote was read')


class ProofMessage(BaseModel):
    message: str


class ReserveRequest(BaseModel):
    minerHotkey: str
    fromChain: str
    toChain: str
    taoAmount: int
    fromAmount: int
    toAmount: int
    fromAddress: str
    fromAddressProof: str
    blockAnchor: int
    expectedRate: str


class ReserveResponse(BaseModel):
    requestHash: str
    reservedUntilBlock: int
    minerSourceAddress: str
    minerHotkey: str


class ConfirmRequest(BaseModel):
    requestHash: str = Field(
        ..., description='Correlation key from /reserve — informational; validators verify on-chain'
    )
    minerHotkey: str = Field(..., description='Miner that holds the reservation (echoed from /reserve response)')
    fromTxHash: str
    fromTxProof: str
    fromAddress: str
    toAddress: str
    fromChain: str
    toChain: str
    fromTxBlock: int = 0


class ConfirmResponse(BaseModel):
    accepted: bool
    swapId: Optional[int] = None
    rejection: Optional[str] = None


class RateChangedError(BaseModel):
    code: str = 'RateChanged'
    expected: str
    actual: str
