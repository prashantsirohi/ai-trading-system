import { Component, ReactNode } from 'react';
import PageFrame from '@/components/common/PageFrame';

interface Props {
  children: ReactNode;
  title: string;
  description?: string;
}

interface State {
  hasError: boolean;
  error?: Error;
}

export default class PageErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  render() {
    if (this.state.hasError) {
      return (
        <PageFrame title={this.props.title} description={this.props.description || ''}>
          <div className="rounded-lg border border-red-900/50 bg-red-950/30 p-6">
            <h2 className="text-lg font-medium text-red-200">Something went wrong</h2>
            <p className="mt-2 text-sm text-red-300">
              {this.state.error?.message || 'An unexpected error occurred'}
            </p>
            <button
              onClick={() => window.location.reload()}
              className="mt-4 rounded-md bg-red-800 px-4 py-2 text-sm text-white hover:bg-red-700"
            >
              Reload Page
            </button>
          </div>
        </PageFrame>
      );
    }

    return this.props.children;
  }
}