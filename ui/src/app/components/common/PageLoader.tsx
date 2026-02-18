import React from 'react';

interface PageLoaderProps {
    /**
     * Optional text to display below the spinner.
     * Defaults to "Loading..." if not provided, or can be set to null/empty to hide.
     */
    text?: string | null;
    /**
     * Optional className to override or extend the container styling.
     */
    className?: string;
}

export function PageLoader({ text = 'Loading...', className = '' }: PageLoaderProps) {
    return (
        <div
            className={`flex items-center justify-center h-[calc(100vh-100px)] w-full ${className}`}
            data-testid="page-loader"
        >
            <div className="flex flex-col items-center gap-4">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
                {text && (
                    <p className="text-muted-foreground text-sm font-mono tracking-widest uppercase animate-pulse">
                        {text}
                    </p>
                )}
            </div>
        </div>
    );
}
