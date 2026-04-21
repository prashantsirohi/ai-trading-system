import type { RankingResponse } from '@/types/api';

export const rankingMock: RankingResponse = {
  rows: [
    { symbol: 'RELIANCE', score: 8.92, rs: 92, volume: 'High', sector: 'Energy', breakout: true, pattern: 'Cup & Handle', tier: 'A', price: 2945.5, sectorStrength: 88, trend: 84 },
    { symbol: 'HDFCBANK', score: 8.45, rs: 88, volume: 'Medium', sector: 'Banking', breakout: false, pattern: 'N/A', tier: 'B', price: 1772.9, sectorStrength: 83, trend: 78 },
    { symbol: 'INFY', score: 8.21, rs: 85, volume: 'High', sector: 'IT', breakout: true, pattern: 'Round Bottom', tier: 'A', price: 1648.2, sectorStrength: 79, trend: 82 },
    { symbol: 'BEL', score: 7.98, rs: 81, volume: 'High', sector: 'Defence', breakout: true, pattern: 'Tight Flag', tier: 'A', price: 326.1, sectorStrength: 90, trend: 80 },
    { symbol: 'LT', score: 7.74, rs: 79, volume: 'Medium', sector: 'Infra', breakout: false, pattern: 'Flat Base', tier: 'B', price: 3866.5, sectorStrength: 76, trend: 74 },
    { symbol: 'TATAPOWER', score: 7.41, rs: 74, volume: 'Low', sector: 'Power', breakout: false, pattern: 'Round Bottom', tier: 'B', price: 468.3, sectorStrength: 81, trend: 70 }
  ]
};
