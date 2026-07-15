import { lazy, Suspense } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import { AuthProvider, LoginView, useAuth } from './auth';
import { AppShell } from './components';
import { usePhase4Query } from './queries';
import type { JsonRecord } from './types';

const OverviewPage = lazy(() => import('./pages').then((module) => ({ default: module.OverviewPage })));
const MarketPage = lazy(() => import('./pages').then((module) => ({ default: module.MarketPage })));
const RoutingPage = lazy(() => import('./pages').then((module) => ({ default: module.RoutingPage })));
const CandidatesPage = lazy(() => import('./pages').then((module) => ({ default: module.CandidatesPage })));
const PositionsPage = lazy(() => import('./pages').then((module) => ({ default: module.PositionsPage })));
const AlertsPage = lazy(() => import('./pages').then((module) => ({ default: module.AlertsPage })));
const GovernancePage = lazy(() => import('./pages').then((module) => ({ default: module.GovernancePage })));
const CalibrationPage = lazy(() => import('./pages').then((module) => ({ default: module.CalibrationPage })));
const PerformancePage = lazy(() => import('./pages').then((module) => ({ default: module.PerformancePage })));
const ReadinessPage = lazy(() => import('./pages').then((module) => ({ default: module.ReadinessPage })));
const DetailPage = lazy(() => import('./pages').then((module) => ({ default: module.DetailPage })));

function AuthenticatedApp() {
  const auth = useAuth();
  const readiness = usePhase4Query<JsonRecord>('/api/v1/system/readiness', { enabled: Boolean(auth.credential), poll: true });
  if (!auth.credential) return <LoginView />;
  return <AppShell readiness={readiness.data?.data}><Suspense fallback={<div className="loading" role="status">Loading dashboard view…</div>}>
    <Routes>
      <Route path="/" element={<OverviewPage />} />
      <Route path="/market" element={<MarketPage />} />
      <Route path="/routing" element={<RoutingPage />} />
      <Route path="/routing/:id" element={<DetailPage kind="routing" />} />
      <Route path="/candidates" element={<CandidatesPage />} />
      <Route path="/candidates/:id" element={<DetailPage kind="candidates" />} />
      <Route path="/positions" element={<PositionsPage />} />
      <Route path="/positions/:id" element={<DetailPage kind="positions" />} />
      <Route path="/alerts" element={<AlertsPage />} />
      <Route path="/alerts/:id" element={<DetailPage kind="alerts" />} />
      <Route path="/incidents/:id" element={<DetailPage kind="incidents" />} />
      <Route path="/governance" element={<GovernancePage />} />
      <Route path="/calibration" element={<CalibrationPage />} />
      <Route path="/performance" element={<PerformancePage />} />
      <Route path="/readiness" element={<ReadinessPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes></Suspense>
  </AppShell>;
}

export default function Phase4App() {
  return <AuthProvider><AuthenticatedApp /></AuthProvider>;
}
