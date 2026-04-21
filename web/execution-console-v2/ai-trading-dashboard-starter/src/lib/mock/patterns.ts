import type { PatternResponse } from '@/types/api';
import { rankingMock } from './ranking';

export const patternsMock: PatternResponse = {
  rows: rankingMock.rows.filter((row) => row.pattern !== 'N/A'),
};
