import { NavLink } from 'react-router-dom';
import {
  BeakerIcon,
  ChartBarIcon,
  CpuChipIcon,
  HomeIcon,
  PlayIcon,
  PresentationChartLineIcon,
  RocketLaunchIcon,
  Squares2X2Icon,
} from '@heroicons/react/24/outline';
import { cn } from '@/lib/utils/cn';

const items = [
  { to: '/pipeline', label: 'Pipeline', icon: HomeIcon },
  { to: '/ranking', label: 'Ranking', icon: ChartBarIcon },
  { to: '/patterns', label: 'Patterns', icon: PresentationChartLineIcon },
  { to: '/sectors', label: 'Sectors', icon: Squares2X2Icon },
  { to: '/execution', label: 'Execution', icon: RocketLaunchIcon },
  { to: '/runs', label: 'Runs', icon: PlayIcon },
  { to: '/shadow', label: 'Shadow', icon: CpuChipIcon },
  { to: '/research', label: 'Research', icon: BeakerIcon },
];

export default function Sidebar() {
  return (
    <aside className="hidden w-72 border-r border-slate-800 bg-slate-950 p-4 lg:block">
      <div className="mb-6 rounded-3xl border border-slate-800 bg-slate-900 p-4 shadow-soft">
        <div className="text-sm text-slate-400">AI Trading System</div>
        <div className="mt-1 text-xl font-semibold">Operator Console</div>
      </div>

      <nav className="space-y-2">
        {items.map((item) => {
          const Icon = item.icon;
          return (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-2xl border px-4 py-3 transition',
                  isActive
                    ? 'border-blue-500/40 bg-blue-500/10 text-white'
                    : 'border-slate-800 bg-slate-900/50 text-slate-300 hover:bg-slate-900 hover:text-white'
                )
              }
            >
              <Icon className="h-5 w-5" />
              <span>{item.label}</span>
            </NavLink>
          );
        })}
      </nav>
    </aside>
  );
}
