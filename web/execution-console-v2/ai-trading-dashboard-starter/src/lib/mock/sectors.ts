import type { SectorResponse } from '@/types/api';

export const sectorsMock: SectorResponse = {
  sectors: [
    { sector: 'Defence', rs: 90, rs20: 85, rs50: 88, rs100: 90, momentum: 0.5, rank: 1, rankPct: 0.05, momentumRank: 2, quadrant: 'Leading' },
    { sector: 'Energy', rs: 88, rs20: 82, rs50: 85, rs100: 87, momentum: 0.3, rank: 2, rankPct: 0.10, momentumRank: 4, quadrant: 'Leading' },
    { sector: 'Banking', rs: 83, rs20: 80, rs50: 78, rs100: 81, momentum: 0.2, rank: 3, rankPct: 0.15, momentumRank: 6, quadrant: 'Leading' },
    { sector: 'Power', rs: 81, rs20: 76, rs50: 79, rs100: 80, momentum: 0.1, rank: 4, rankPct: 0.20, momentumRank: 8, quadrant: 'Leading' },
    { sector: 'IT', rs: 79, rs20: 75, rs50: 77, rs100: 78, momentum: 0.05, rank: 5, rankPct: 0.25, momentumRank: 10, quadrant: 'Lagging' },
    { sector: 'Infra', rs: 76, rs20: 72, rs50: 74, rs100: 75, momentum: 0, rank: 6, rankPct: 0.30, momentumRank: 12, quadrant: 'Lagging' },
  ],
};