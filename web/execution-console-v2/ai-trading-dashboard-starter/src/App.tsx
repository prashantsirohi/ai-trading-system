import { Routes, Route, Navigate } from 'react-router-dom';
import { AnimatePresence } from 'framer-motion';
import AppLayout from '@/components/layout/AppLayout';
import PipelinePage from '@/pages/PipelinePage';
import RankingPage from '@/pages/RankingPage';
import PatternsPage from '@/pages/PatternsPage';
import SectorsPage from '@/pages/SectorsPage';
import ExecutionPage from '@/pages/ExecutionPage';
import RunsPage from '@/pages/RunsPage';
import ShadowPage from '@/pages/ShadowPage';
import ResearchPage from '@/pages/ResearchPage';

export default function App() {
  return (
    <AppLayout>
      <AnimatePresence mode="wait">
        <Routes>
          <Route path="/" element={<Navigate to="/pipeline" replace />} />
          <Route path="/pipeline" element={<PipelinePage />} />
          <Route path="/ranking" element={<RankingPage />} />
          <Route path="/patterns" element={<PatternsPage />} />
          <Route path="/sectors" element={<SectorsPage />} />
          <Route path="/execution" element={<ExecutionPage />} />
          <Route path="/runs" element={<RunsPage />} />
          <Route path="/shadow" element={<ShadowPage />} />
          <Route path="/research" element={<ResearchPage />} />
        </Routes>
      </AnimatePresence>
    </AppLayout>
  );
}
