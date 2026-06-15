import { Routes, Route, Navigate } from 'react-router-dom';
import { AnimatePresence } from 'framer-motion';
import AppLayout from '@/components/layout/AppLayout';
import ControlTowerPage from '@/pages/ControlTowerPage';
import PipelinePage from '@/pages/PipelinePage';
import RankingPage from '@/pages/RankingPage';
import PatternsPage from '@/pages/PatternsPage';
import SectorsPage from '@/pages/SectorsPage';
import SectorRotationPage from '@/pages/SectorRotationPage';
import ExecutionPage from '@/pages/ExecutionPage';
import RunsPage from '@/pages/RunsPage';
import ShadowPage from '@/pages/ShadowPage';
import ResearchPage from '@/pages/ResearchPage';
import BacktestPage from '@/pages/BacktestPage';
import OptimizationPage from '@/pages/OptimizationPage';
import WatchlistPage from '@/pages/WatchlistPage';
import RiskPage from '@/pages/RiskPage';
import SectorDetailPage from '@/pages/SectorDetailPage';
import SymbolPage from '@/pages/SymbolPage';
import FundamentalsPage from '@/pages/FundamentalsPage';
import InvestigatorPage from '@/pages/InvestigatorPage';

export default function App() {
  return (
    <AppLayout>
      <AnimatePresence mode="wait">
        <Routes>
          <Route path="/" element={<ControlTowerPage />} />
          <Route path="/symbol/:sym" element={<SymbolPage />} />
          <Route path="/pipeline" element={<PipelinePage />} />
          <Route path="/ranking" element={<RankingPage />} />
          <Route path="/patterns" element={<PatternsPage />} />
          <Route path="/sectors" element={<SectorsPage />} />
          <Route path="/sector-rotation" element={<SectorRotationPage />} />
          <Route path="/fundamentals" element={<FundamentalsPage />} />
          <Route path="/investigator" element={<InvestigatorPage />} />
          <Route path="/sectors/:sector" element={<SectorDetailPage />} />
          <Route path="/execution" element={<ExecutionPage />} />
          <Route path="/runs" element={<RunsPage />} />
          <Route path="/shadow" element={<ShadowPage />} />
          <Route path="/research" element={<ResearchPage />} />
          <Route path="/backtest" element={<BacktestPage />} />
          <Route path="/optimization" element={<OptimizationPage />} />
          <Route path="/watchlist" element={<WatchlistPage />} />
          <Route path="/risk" element={<RiskPage />} />
          {/* Catch-all keeps direct deep-links resilient when a page is renamed. */}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AnimatePresence>
    </AppLayout>
  );
}
