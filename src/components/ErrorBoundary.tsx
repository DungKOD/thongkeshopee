import { Component, type ReactNode, type ErrorInfo } from "react";

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  error: Error | null;
  info: ErrorInfo | null;
}

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = { error: null, info: null };

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("[ErrorBoundary]", error, info);
    this.setState({ error, info });
  }

  reset = () => this.setState({ error: null, info: null });

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <div className="min-h-screen bg-gray-950 p-6 text-gray-100">
        <div className="mx-auto max-w-3xl rounded-lg border border-red-600 bg-gray-900 p-6">
          <h1 className="mb-3 text-xl font-bold text-red-400">
            Lỗi ứng dụng
          </h1>
          <pre className="mb-3 whitespace-pre-wrap rounded bg-gray-950 p-3 text-sm text-red-300">
            {this.state.error.message}
          </pre>
          {this.state.error.stack && (
            <pre className="mb-3 max-h-80 overflow-auto whitespace-pre-wrap rounded bg-gray-950 p-3 text-xs text-gray-400">
              {this.state.error.stack}
            </pre>
          )}
          <button
            onClick={this.reset}
            className="rounded-md bg-shopee-500 px-4 py-2 text-sm font-semibold text-white hover:bg-shopee-600"
          >
            Thử lại
          </button>
        </div>
      </div>
    );
  }
}
