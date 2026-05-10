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
    withStage({ sector: 'Defence', rs: 90, rs20: 85, rs50: 88, rs100: 90, momentum: 0.5, rank: 1, rankPct: 0.05, momentumRank: 2, quadrant: 'Leading' }),
    withStage({ sector: 'Energy', rs: 88, rs20: 82, rs50: 85, rs100: 87, momentum: 0.3, rank: 2, rankPct: 0.10, momentumRank: 4, quadrant: 'Leading' }),
    withStage({ sector: 'Banking', rs: 83, rs20: 80, rs50: 78, rs100: 81, momentum: 0.2, rank: 3, rankPct: 0.15, momentumRank: 6, quadrant: 'Leading' }),
    withStage({ sector: 'Power', rs: 81, rs20: 76, rs50: 79, rs100: 80, momentum: 0.1, rank: 4, rankPct: 0.20, momentumRank: 8, quadrant: 'Leading' }),
    withStage({ sector: 'IT', rs: 79, rs20: 75, rs50: 77, rs100: 78, momentum: 0.05, rank: 5, rankPct: 0.25, momentumRank: 10, quadrant: 'Lagging' }),
    withStage({ sector: 'Infra', rs: 76, rs20: 72, rs50: 74, rs100: 75, momentum: 0, rank: 6, rankPct: 0.30, momentumRank: 12, quadrant: 'Lagging' }),
  ],
};
