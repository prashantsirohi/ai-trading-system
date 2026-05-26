import type { SectorResponse } from '@/types/api';
import type { SectorScore } from '@/types/dashboard';

function withStage(row: Omit<SectorScore, 'stageS1Pct' | 'stageS2Pct' | 'stageS3Pct' | 'stageS4Pct' | 'stageS1Count' | 'stageS2Count' | 'stageS3Count' | 'stageS4Count' | 'stageTotal'>): SectorScore {
  return {
    ...row,
    stageS1Pct: 24,
    stageS2Pct: 52,
    stageS3Pct: 14,
    stageS4Pct: 10,
    stageS1Count: 12,
    stageS2Count: 26,
    stageS3Count: 7,
    stageS4Count: 5,
    stageTotal: 50,
  };
}

export const sectorsMock: SectorResponse = {
  sectors: [
    withStage({ sector: 'Auto', rs: 90, rs20: 85, rs50: 88, rs100: 90, momentum: 0.5, rank: 2, rankPct: 0.05, momentumRank: 2, quadrant: 'Leading', sectorPeTtm: 31, sectorPePctile5y: 88, valuationZone: 'expensive', valuationInterpretation: 'Strong but late-cycle', sectorEarningsGrowthScore: 84, sectorSalesYoyGrowth: 0.22, sectorProfitYoyGrowth: 0.34, sectorSalesQoqGrowth: 0.06, sectorProfitQoqGrowth: 0.09, salesYoyPositivePct: 78, profitYoyPositivePct: 72, marginExpansionPct: 65, earningsTrendLabel: 'accelerating_leader' }),
    withStage({ sector: 'IT', rs: 79, rs20: 75, rs50: 77, rs100: 78, momentum: 0.05, rank: 9, rankPct: 0.45, momentumRank: 10, quadrant: 'Lagging', sectorPeTtm: 23, sectorPePctile5y: 35, valuationZone: 'fair', valuationInterpretation: 'Valuation reset, wait for RS turn', sectorEarningsGrowthScore: 43, sectorSalesYoyGrowth: 0.07, sectorProfitYoyGrowth: 0.05, sectorSalesQoqGrowth: 0.02, sectorProfitQoqGrowth: 0.01, salesYoyPositivePct: 48, profitYoyPositivePct: 42, marginExpansionPct: 35, earningsTrendLabel: 'growth_but_margin_pressure' }),
    withStage({ sector: 'PSU Bank', rs: 88, rs20: 82, rs50: 85, rs100: 87, momentum: 0.3, rank: 1, rankPct: 0.10, momentumRank: 4, quadrant: 'Leading', sectorPeTtm: 10, sectorPePctile5y: 72, valuationZone: 'fair', valuationInterpretation: 'Momentum strong, not cheap', sectorEarningsGrowthScore: 76, sectorSalesYoyGrowth: 0.16, sectorProfitYoyGrowth: 0.27, sectorSalesQoqGrowth: 0.04, sectorProfitQoqGrowth: 0.07, salesYoyPositivePct: 70, profitYoyPositivePct: 68, marginExpansionPct: 58, earningsTrendLabel: 'accelerating_leader' }),
    withStage({ sector: 'Pharma', rs: 83, rs20: 80, rs50: 78, rs100: 81, momentum: 0.2, rank: 4, rankPct: 0.15, momentumRank: 6, quadrant: 'Leading', sectorPeTtm: 37, sectorPePctile5y: 92, valuationZone: 'expensive', valuationInterpretation: 'Needs earnings growth confirmation', sectorEarningsGrowthScore: 66, sectorSalesYoyGrowth: 0.11, sectorProfitYoyGrowth: 0.18, sectorSalesQoqGrowth: 0.03, sectorProfitQoqGrowth: 0.04, salesYoyPositivePct: 62, profitYoyPositivePct: 60, marginExpansionPct: 54, earningsTrendLabel: 'earnings_recovery' }),
    withStage({ sector: 'Power', rs: 81, rs20: 76, rs50: 79, rs100: 80, momentum: 0.1, rank: 5, rankPct: 0.20, momentumRank: 8, quadrant: 'Leading', sectorPeTtm: 18, sectorPePctile5y: 44, valuationZone: 'fair', valuationInterpretation: 'Momentum and valuation are balanced', sectorEarningsGrowthScore: 80, sectorSalesYoyGrowth: 0.18, sectorProfitYoyGrowth: 0.29, sectorSalesQoqGrowth: 0.05, sectorProfitQoqGrowth: 0.07, salesYoyPositivePct: 74, profitYoyPositivePct: 68, marginExpansionPct: 61, earningsTrendLabel: 'accelerating_leader' }),
    withStage({ sector: 'Infra', rs: 76, rs20: 72, rs50: 74, rs100: 75, momentum: 0, rank: 6, rankPct: 0.30, momentumRank: 12, quadrant: 'Lagging', sectorPeTtm: 29, sectorPePctile5y: 77, valuationZone: 'fair', valuationInterpretation: 'Momentum and valuation are balanced', sectorEarningsGrowthScore: 38, sectorSalesYoyGrowth: -0.02, sectorProfitYoyGrowth: -0.09, sectorSalesQoqGrowth: 0.01, sectorProfitQoqGrowth: 0.03, salesYoyPositivePct: 32, profitYoyPositivePct: 28, marginExpansionPct: 30, earningsTrendLabel: 'weak_or_declining' }),
  ],
};
