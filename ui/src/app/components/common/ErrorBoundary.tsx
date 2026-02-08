import React, { Component, ErrorInfo, ReactNode } from 'react';
import { Button } from '@/app/components/ui/button';
import { RefreshCw, AlertTriangle } from 'lucide-react';

interface Props {
    children: ReactNode;
    fallback?: ReactNode;
    onReset?: () => void;
}

interface State {
    hasError: boolean;
    error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
    constructor(props: Props) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error: Error): State {
        return { hasError: true, error };
    }

    componentDidCatch(error: Error, errorInfo: ErrorInfo) {
        console.error('ErrorBoundary caught an error:', error, errorInfo);
    }

    handleReset = () => {
        this.setState({ hasError: false, error: null });
        this.props.onReset?.();
    };

    render() {
        if (this.state.hasError) {
            if (this.props.fallback) {
                return this.props.fallback;
            }

            return (
                <div className="p-6 rounded-lg border border-red-200 bg-red-50 text-red-900 flex flex-col items-center justify-center gap-4 min-h-[200px]">
                    <div className="flex items-center gap-2 text-lg font-semibold">
                        <AlertTriangle className="h-6 w-6 text-red-600" />
                        <span>Something went wrong</span>
                    </div>
                    <p className="text-sm text-red-700 max-w-md text-center">
                        {this.state.error?.message || 'An unexpected error occurred while loading this section.'}
                    </p>
                    <Button
                        variant="outline"
                        onClick={this.handleReset}
                        className="border-red-200 hover:bg-red-100 text-red-800"
                    >
                        <RefreshCw className="mr-2 h-4 w-4" />
                        Try Again
                    </Button>
                </div>
            );
        }

        return this.props.children;
    }
}
