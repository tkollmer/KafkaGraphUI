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
      const isBright = document.documentElement.classList.contains("theme-bright");
      return (
        <div className="flex-1 flex items-center justify-center" style={{ background: isBright ? "#f1f5f9" : "#030712" }}>
          <div className={`rounded-2xl border p-8 max-w-md text-center ${
            isBright ? "border-red-200 bg-red-50/50" : "border-red-500/30 bg-red-950/30"
          }`}>
            <div className={`text-lg font-bold mb-2 ${isBright ? "text-red-700" : "text-red-400"}`}>Something went wrong</div>
            <div className={`text-sm mb-4 ${isBright ? "text-slate-600" : "text-slate-400"}`}>{this.state.error}</div>
            <div className="flex items-center justify-center gap-2">
              <button
                onClick={() => this.setState({ hasError: false, error: "" })}
                className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
                  isBright
                    ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50"
                    : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
                }`}
              >
                Try Again
              </button>
              <button
                onClick={() => window.location.reload()}
                className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
                  isBright
                    ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                    : "bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30"
                }`}
              >
                Reload Page
              </button>
            </div>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
