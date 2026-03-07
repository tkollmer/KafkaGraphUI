import { Component, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: string;
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: "" };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error: error.message };
  }

  componentDidCatch(error: Error, info: { componentStack?: string | null }) {
    console.error("React ErrorBoundary caught:", error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex-1 flex items-center justify-center bg-[#030712]">
          <div className="rounded-2xl border border-red-500/30 bg-red-950/30 p-8 max-w-md text-center">
            <div className="text-red-400 text-lg font-bold mb-2">Something went wrong</div>
            <div className="text-slate-400 text-sm mb-4">{this.state.error}</div>
            <button
              onClick={() => this.setState({ hasError: false, error: "" })}
              className="px-4 py-2 rounded-xl text-sm font-medium bg-slate-800 border border-slate-700/50 text-slate-300 hover:bg-slate-700 transition-colors cursor-pointer"
            >
              Try Again
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
